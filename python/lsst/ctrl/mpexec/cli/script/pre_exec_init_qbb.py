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

from lsst.pipe.base import BuildId, QuantumGraph
from lsst.pipe.base.pipeline_graph import TaskImportMode
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.logging import getLogger

from ..butler_factory import ButlerFactory

_LOG = getLogger(__name__)


def pre_exec_init_qbb(
    butler_config: str,
    qgraph: ResourcePathExpression,
    qgraph_id: str | None,
    config_search_path: list[str] | None,
    **kwargs: object,
) -> None:
    """Implement the command line interface ``pipetask pre-exec-init-qbb``
    subcommand.

    Should only be called by command line tools and unit test code
    that tests this function.

    Parameters
    ----------
    butler_config : `str`
        The path location of the gen3 butler/registry config file.
    qgraph : `str`
        URI location for a serialized quantum graph definition.
    qgraph_id : `str` or `None`
        Quantum graph identifier, if specified must match the identifier of the
        graph loaded from a file. Ignored if graph is not loaded from a file.
    config_search_path : `list` [`str`]
        Additional search paths for butler configuration.
    **kwargs : `object`
        Ignored; click commands may accept options for more than one script
        function and pass all the option kwargs to each of the script functions
        which ignore these unused kwargs.
    """
    qgraph = ResourcePath(qgraph)
    match qgraph.getExtension():
        case ".qgraph":
            _LOG.verbose("Reading full quantum graph from %s.", qgraph)
            qg = PredictedQuantumGraph.from_old_quantum_graph(
                QuantumGraph.loadUri(
                    qgraph,
                    graphID=BuildId(qgraph_id) if qgraph_id is not None else None,
                )
            )
        case ".qg":
            _LOG.verbose("Reading init quanta from quantum graph from %s.", qgraph)
            if qgraph_id is not None:
                _LOG.warning("--qgraph-id is ignored when loading new '.qg' files.")
            with PredictedQuantumGraph.open(
                qgraph, import_mode=TaskImportMode.ASSUME_CONSISTENT_EDGES
            ) as reader:
                reader.read_init_quanta()
            qg = reader.finish()
        case ext:
            raise ValueError(f"Unrecognized extension for quantum graph: {ext!r}")

    # Ensure that QBB uses shared datastore cache for writes.
    ButlerFactory.define_datastore_cache()

    # Make QBB.
    _LOG.verbose("Initializing quantum-backed butler.")
    with qg.make_init_qbb(butler_config, config_search_paths=config_search_path) as butler:
        # Save all InitOutputs, configs, etc.
        _LOG.verbose("Instantiating tasks and saving init-outputs.")
        qg.init_output_run(butler)
