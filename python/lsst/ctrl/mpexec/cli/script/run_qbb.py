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

from __future__ import annotations

import pickle
import uuid
from collections.abc import Mapping
from typing import Literal

import astropy.units as u

import lsst.utils.timer
from lsst.daf.butler import (
    DatasetType,
    DimensionConfig,
    DimensionUniverse,
    LimitedButler,
    Quantum,
    QuantumBackedButler,
)
from lsst.pipe.base import ExecutionResources, TaskFactory
from lsst.pipe.base.mp_graph_executor import MPGraphExecutor
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.logging import VERBOSE, getLogger
from lsst.utils.threads import disable_implicit_threading

from ..butler_factory import ButlerFactory
from ..utils import MP_TIMEOUT, summarize_quantum_graph

_LOG = getLogger(__name__)


def run_qbb(
    *,
    task_factory: TaskFactory | None = None,
    butler_config: ResourcePathExpression,
    qgraph: ResourcePathExpression,
    config_search_path: list[str] | None,
    qgraph_id: str | None,
    qgraph_node_id: list[str | uuid.UUID] | None,
    processes: int,
    pdb: str | None,
    profile: str | None,
    debug: bool,
    start_method: Literal["spawn", "forkserver"] | None,
    timeout: int | None,
    fail_fast: bool,
    summary: ResourcePathExpression | None,
    enable_implicit_threading: bool,
    cores_per_quantum: int,
    memory_per_quantum: str,
    raise_on_partial_outputs: bool,
    no_existing_outputs: bool,
    **kwargs: object,
) -> None:
    """Implement the command line interface ``pipetask run-qbb`` subcommand.

    Should only be called by command line tools and unit test code that tests
    this function.

    Parameters
    ----------
    task_factory : `lsst.pipe.base.TaskFactory`, optional
        A custom task factory to use.
    butler_config : `str`
        The path location of the gen3 butler/registry config file.
    qgraph : `str`
        URI location for a serialized quantum graph definition.
    config_search_path : `list` [`str`]
        Additional search paths for butler configuration.
    qgraph_id : `str` or `None`
        Quantum graph identifier, if specified must match the identifier of the
        graph loaded from a file. Ignored if graph is not loaded from a file.
    qgraph_node_id : `iterable` of `int`, or `None`
        Only load a specified set of nodes if graph is loaded from a file,
        nodes are identified by integer IDs.
    processes : `int`
        The number of processes to use.
    pdb : `str` or `None`
        Debugger to launch for exceptions.
    profile : `str`
        File name to dump cProfile information to.
    debug : `bool`
        If true, enable debugging output using lsstDebug facility (imports
        debug.py).
    start_method : `str` or `None`
        Start method from `multiprocessing` module, `None` selects the best
        one for current platform.
    timeout : `int`
        Timeout for multiprocessing; maximum wall time (sec).
    fail_fast : `bool`
        If true then stop processing at first error, otherwise process as many
        tasks as possible.
    summary : `str` or `None`
        File path to store job report in JSON format.
    enable_implicit_threading : `bool`
        If `True`, do not disable implicit threading by third-party libraries.
        Implicit threading is always disabled during actual quantum execution
        if ``processes > 1``.
    cores_per_quantum : `int`
        Number of cores that can be used by each quantum.
    memory_per_quantum : `str`
        Amount of memory that each quantum can be allowed to use. Empty string
        implies no limit. The string can be either a single integer (implying
        units of MB) or a combination of number and unit.
    raise_on_partial_outputs : `bool`
        Consider partial outputs an error instead of a success.
    no_existing_outputs : `bool`
        Whether to assume that no predicted outputs for these quanta already
        exist in the output run collection.
    **kwargs : `object`
        Ignored; click commands may accept options for more than one script
        function and pass all the option kwargs to each of the script functions
        which ignore these unused kwargs.
    """
    # Fork option still exists for compatibility but we use spawn instead.
    if start_method == "fork":  # type: ignore[comparison-overlap]
        start_method = "spawn"  # type: ignore[unreachable]
        _LOG.warning("Option --start-method=fork is unsafe and no longer supported, using spawn instead.")

    if not enable_implicit_threading:
        disable_implicit_threading()

    # click passes empty tuple as default value for qgraph_node_id
    quantum_ids = (
        {uuid.UUID(q) if not isinstance(q, uuid.UUID) else q for q in qgraph_node_id}
        if qgraph_node_id
        else None
    )
    # Load quantum graph.
    with lsst.utils.timer.time_this(
        _LOG,
        msg=f"Reading {str(len(quantum_ids)) if quantum_ids is not None else 'all'} quanta.",
        level=VERBOSE,
    ) as qg_read_time:
        qg = PredictedQuantumGraph.read_execution_quanta(qgraph, quantum_ids=quantum_ids)
    job_metadata = {"qg_read_time": qg_read_time.duration, "qg_size": len(qg)}

    summarize_quantum_graph(qg.header)

    dataset_types = {dtn.name: dtn.dataset_type for dtn in qg.pipeline_graph.dataset_types.values()}

    # Ensure that QBB uses shared datastore cache.
    ButlerFactory.define_datastore_cache()

    _butler_factory = _QBBFactory(
        butler_config=butler_config,
        dimensions=qg.pipeline_graph.universe,
        dataset_types=dataset_types,
        config_search_path=config_search_path,
    )

    # make special quantum executor
    resources = ExecutionResources(
        num_cores=cores_per_quantum, max_mem=memory_per_quantum, default_mem_units=u.MB
    )
    quantumExecutor = SingleQuantumExecutor(
        butler=None,
        task_factory=task_factory,
        enable_lsst_debug=debug,
        limited_butler_factory=_butler_factory,
        resources=resources,
        assume_no_existing_outputs=no_existing_outputs,
        skip_existing=True,
        clobber_outputs=True,
        raise_on_partial_outputs=raise_on_partial_outputs,
        job_metadata=job_metadata,
    )

    timeout = MP_TIMEOUT if timeout is None else timeout
    executor = MPGraphExecutor(
        num_proc=processes,
        timeout=timeout,
        start_method=start_method,
        quantum_executor=quantumExecutor,
        fail_fast=fail_fast,
        pdb=pdb,
    )
    try:
        with lsst.utils.timer.profile(profile, _LOG):
            executor.execute(qg)
    finally:
        if summary:
            report = executor.getReport()
            if report:
                with ResourcePath(summary).open("w") as out:
                    # Do not save fields that are not set.
                    out.write(report.model_dump_json(exclude_none=True, indent=2))


class _QBBFactory:
    """Class which is a callable for making QBB instances.

    This class is also responsible for reconstructing correct dimension
    universe after unpickling. When pickling multiple things that require
    dimension universe, this class must be unpickled first. The logic in
    MPGraphExecutor ensures that SingleQuantumExecutor is unpickled first in
    the subprocess, which causes unpickling of this class.
    """

    def __init__(
        self,
        butler_config: ResourcePathExpression,
        dimensions: DimensionUniverse,
        dataset_types: Mapping[str, DatasetType],
        config_search_path: list[str] | None,
    ):
        self.butler_config = butler_config
        self.dimensions = dimensions
        self.dataset_types = dataset_types
        self.config_search_path = config_search_path

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
        cls,
        butler_config: ResourcePathExpression,
        dimensions_config: DimensionConfig | None,
        dataset_types_pickle: bytes,
        config_search_path: list[str] | None,
    ) -> _QBBFactory:
        universe = DimensionUniverse(dimensions_config)
        dataset_types = pickle.loads(dataset_types_pickle)
        return _QBBFactory(butler_config, universe, dataset_types, config_search_path)

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
        return (
            self._unpickle,
            (self.butler_config, dimension_config, dataset_types_pickle, self.config_search_path),
        )
