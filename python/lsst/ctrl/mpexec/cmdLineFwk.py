# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module defining CmdLineFwk class and related methods."""

from __future__ import annotations

__all__ = ["CmdLineFwk"]

import pickle
from collections.abc import Mapping
from types import SimpleNamespace

import astropy.units as u

import lsst.utils.timer
from lsst.daf.butler import (
    Config,
    DatasetType,
    DimensionConfig,
    DimensionUniverse,
    LimitedButler,
    Quantum,
    QuantumBackedButler,
)
from lsst.pipe.base import ExecutionResources, QuantumGraph, TaskFactory
from lsst.pipe.base.mp_graph_executor import MPGraphExecutor
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.utils.logging import VERBOSE, getLogger
from lsst.utils.threads import disable_implicit_threading

from .cli.butler_factory import ButlerFactory
from .cli.utils import MP_TIMEOUT
from .preExecInit import PreExecInitLimited

_LOG = getLogger(__name__)


class _QBBFactory:
    """Class which is a callable for making QBB instances.

    This class is also responsible for reconstructing correct dimension
    universe after unpickling. When pickling multiple things that require
    dimension universe, this class must be unpickled first. The logic in
    MPGraphExecutor ensures that SingleQuantumExecutor is unpickled first in
    the subprocess, which causes unpickling of this class.
    """

    def __init__(
        self, butler_config: Config, dimensions: DimensionUniverse, dataset_types: Mapping[str, DatasetType]
    ):
        self.butler_config = butler_config
        self.dimensions = dimensions
        self.dataset_types = dataset_types

    def __call__(self, quantum: Quantum) -> LimitedButler:
        """Return freshly initialized `~lsst.daf.butler.QuantumBackedButler`.

        Factory method to create QuantumBackedButler instances.
        """
        return QuantumBackedButler.initialize(
            config=self.butler_config,
            quantum=quantum,
            dimensions=self.dimensions,
            dataset_types=self.dataset_types,
        )

    @classmethod
    def _unpickle(
        cls, butler_config: Config, dimensions_config: DimensionConfig | None, dataset_types_pickle: bytes
    ) -> _QBBFactory:
        universe = DimensionUniverse(dimensions_config)
        dataset_types = pickle.loads(dataset_types_pickle)
        return _QBBFactory(butler_config, universe, dataset_types)

    def __reduce__(self) -> tuple:
        # If dimension universe is not default one, we need to dump/restore
        # its config.
        config = self.dimensions.dimensionConfig
        default = DimensionConfig()
        # Only send configuration to other side if it is non-default, default
        # will be instantiated from config=None.
        if (config["namespace"], config["version"]) != (default["namespace"], default["version"]):
            dimension_config = config
        else:
            dimension_config = None
        # Dataset types need to be unpickled only after universe is made.
        dataset_types_pickle = pickle.dumps(self.dataset_types)
        return (self._unpickle, (self.butler_config, dimension_config, dataset_types_pickle))


class CmdLineFwk:
    """PipelineTask framework which executes tasks from command line.

    In addition to executing tasks this activator provides additional methods
    for task management like dumping configuration or execution chain.
    """

    def _make_execution_resources(self, args: SimpleNamespace) -> ExecutionResources:
        """Construct the execution resource class from arguments.

        Parameters
        ----------
        args : `types.SimpleNamespace`
            Parsed command line.

        Returns
        -------
        resources : `~lsst.pipe.base.ExecutionResources`
            The resources available to each quantum.
        """
        return ExecutionResources(
            num_cores=args.cores_per_quantum, max_mem=args.memory_per_quantum, default_mem_units=u.MB
        )

    def preExecInitQBB(self, task_factory: TaskFactory, args: SimpleNamespace) -> None:
        _LOG.verbose("Reading full quantum graph from %s.", args.qgraph)
        # Load quantum graph. We do not really need individual Quanta here,
        # but we need datastore records for initInputs, and those are only
        # available from Quanta, so load the whole thing.
        qgraph = QuantumGraph.loadUri(args.qgraph, graphID=args.qgraph_id)

        # Ensure that QBB uses shared datastore cache for writes.
        ButlerFactory.define_datastore_cache()

        # Make QBB.
        _LOG.verbose("Initializing quantum-backed butler.")
        butler = qgraph.make_init_qbb(args.butler_config, config_search_paths=args.config_search_path)
        # Save all InitOutputs, configs, etc.
        _LOG.verbose("Instantiating tasks and saving init-outputs.")
        preExecInit = PreExecInitLimited(butler, task_factory)
        preExecInit.initialize(qgraph)

    def runGraphQBB(self, task_factory: TaskFactory, args: SimpleNamespace) -> None:
        if not args.enable_implicit_threading:
            disable_implicit_threading()

        # Load quantum graph.
        nodes = args.qgraph_node_id or None
        with lsst.utils.timer.time_this(
            _LOG,
            msg=f"Reading {str(len(nodes)) if nodes is not None else 'all'} quanta.",
            level=VERBOSE,
        ) as qg_read_time:
            qgraph = QuantumGraph.loadUri(args.qgraph, nodes=nodes, graphID=args.qgraph_id)
        job_metadata = {"qg_read_time": qg_read_time.duration, "qg_size": len(qgraph)}

        if qgraph.metadata is None:
            raise ValueError("QuantumGraph is missing metadata, cannot continue.")

        from .cli.utils import summarize_quantum_graph

        summarize_quantum_graph(qgraph)

        dataset_types = {dstype.name: dstype for dstype in qgraph.registryDatasetTypes()}

        # Ensure that QBB uses shared datastore cache.
        ButlerFactory.define_datastore_cache()

        _butler_factory = _QBBFactory(
            butler_config=args.butler_config,
            dimensions=qgraph.universe,
            dataset_types=dataset_types,
        )

        # make special quantum executor
        resources = self._make_execution_resources(args)
        quantumExecutor = SingleQuantumExecutor(
            butler=None,
            task_factory=task_factory,
            enable_lsst_debug=args.enableLsstDebug,
            limited_butler_factory=_butler_factory,
            resources=resources,
            assume_no_existing_outputs=args.no_existing_outputs,
            skip_existing=True,
            clobber_outputs=True,
            raise_on_partial_outputs=args.raise_on_partial_outputs,
            job_metadata=job_metadata,
        )

        timeout = MP_TIMEOUT if args.timeout is None else args.timeout
        executor = MPGraphExecutor(
            num_proc=args.processes,
            timeout=timeout,
            start_method=args.start_method,
            quantum_executor=quantumExecutor,
            fail_fast=args.fail_fast,
            pdb=args.pdb,
        )
        try:
            with lsst.utils.timer.profile(args.profile, _LOG):
                executor.execute(qgraph)
        finally:
            if args.summary:
                report = executor.getReport()
                if report:
                    with open(args.summary, "w") as out:
                        # Do not save fields that are not set.
                        out.write(report.model_dump_json(exclude_none=True, indent=2))
