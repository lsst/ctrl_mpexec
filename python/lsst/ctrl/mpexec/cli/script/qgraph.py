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

__all__ = ("qgraph",)

import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING

from astropy.table import Table

from lsst.pipe.base import BuildId, QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import (
    AllDimensionsQuantumGraphBuilder,
    DatasetQueryConstraintVariant,
)
from lsst.pipe.base.dot_tools import graph2dot
from lsst.pipe.base.mermaid_tools import graph2mermaid
from lsst.pipe.base.pipeline_graph import TaskImportMode
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph, PredictedQuantumGraphComponents
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.iteration import ensure_iterable
from lsst.utils.logging import getLogger

from ..._pipeline_graph_factory import PipelineGraphFactory
from ...showInfo import ShowInfo
from ..butler_factory import ButlerFactory
from ..utils import summarize_quantum_graph

if TYPE_CHECKING:
    from lsst.pipe.base.tests.mocks import ForcedFailure  # this monkey patches; only import for annotation!

_LOG = getLogger(__name__)


def qgraph(
    pipeline_graph_factory: PipelineGraphFactory | None,
    *,
    qgraph: ResourcePathExpression | None,
    qgraph_id: str | None,
    qgraph_node_id: Iterable[uuid.UUID | str] | None,
    qgraph_datastore_records: bool,
    skip_existing_in: Iterable[str] | None,
    skip_existing: bool,
    save_qgraph: ResourcePathExpression | None,
    qgraph_dot: str | None,
    qgraph_mermaid: str | None,
    butler_config: ResourcePathExpression,
    input: Iterable[str] | str,
    output: str | None,
    output_run: str | None,
    extend_run: bool,
    replace_run: bool,
    prune_replaced: str | None,
    data_query: str | None,
    data_id_table: Iterable[ResourcePathExpression],
    show: ShowInfo,
    clobber_outputs: bool,
    dataset_query_constraint: str,
    rebase: bool,
    mock: bool = False,
    unmocked_dataset_types: Sequence[str],
    mock_failure: Mapping[str, ForcedFailure],
    for_execution: bool = False,
    for_init_output_run: bool = False,
    **kwargs: object,
) -> PredictedQuantumGraph | None:
    """Implement the command line interface `pipetask qgraph` subcommand.

    Should only be called by command line tools and unit test code that test
    this function.

    Parameters
    ----------
    pipeline_graph_factory : `..PipelineGraphFactory` or `None`
        A factory that holds the pipeline and can produce a pipeline graph.
        If this is not `None` then `qgraph` should be `None`.
    qgraph : convertible to `lsst.resources.ResourcePath`, or `None`
        URI location for a serialized quantum graph definition. If this option
        is not `None` then ``pipeline_graph_factory`` should be `None`.
    qgraph_id : `str` or `None`
        Quantum graph identifier, if specified must match the identifier of the
        graph loaded from a file. Ignored if graph is not loaded from a file.
    qgraph_node_id : `~collections.abc.Iterable` [`str` | `uuid.UUID`] or \
            `None`
        Only load a specified set of nodes if graph is loaded from a file,
        nodes are identified by integer IDs.
    qgraph_datastore_records : `bool`
        If `True` then include datastore records into generated quanta.
    skip_existing_in : `~collections.abc.Iterable` [ `str` ] or `None`
        Accepts list of collections, if all Quantum outputs already exist in
        the specified list of collections then that Quantum will be excluded
        from the QuantumGraph.
    skip_existing : `bool`
        Appends output RUN collection to the ``skip_existing_in`` list.
    save_qgraph : convertible to `lsst.resources.ResourcePath` or `None`
        URI location for saving the quantum graph.
    qgraph_dot : `str` or `None`
        Path location for storing GraphViz DOT representation of a quantum
        graph.
    qgraph_mermaid : `str` or `None`
        Path location for storing Mermaid representation of a quantum graph.
    butler_config : convertible to `lsst.resources.ResourcePath`
        Path to butler repository configuration.
    input : `~collections.abc.Iterable` [ `str` ] or `None`
        List of names of the input collection(s).
    output : `str` or `None`
        Name of the output CHAINED collection. This may either be an existing
        CHAINED collection to use as both input and output (if `input` is
        `None`), or a new CHAINED collection created to include all inputs
        (if `input` is not `None`). In both cases, the collection's children
        will start with an output RUN collection that directly holds all new
        datasets (see `output_run`).
    output_run : `str` or `None`
        Name of the new output RUN collection. If not provided then `output`
        must be provided and a new RUN collection will be created by appending
        a timestamp to the value passed with `output`. If this collection
        already exists then `extend_run` must be passed.
    extend_run : `bool`
        Instead of creating a new RUN collection, insert datasets into either
        the one given by `output_run` (if provided) or the first child
        collection of `output` (which must be of type RUN).
    replace_run : `bool`
        Before creating a new RUN collection in an existing CHAINED collection,
        remove the first child collection (which must be of type RUN). This can
        be used to repeatedly write to the same (parent) collection during
        development, but it does not delete the datasets associated with the
        replaced run unless `prune-replaced` is also True. Requires `output`,
        and `extend_run` must be `None`.
    prune_replaced : `str` or `None`
        If not `None`, delete the datasets in the collection replaced by
        `replace_run`, either just from the datastore ("unstore") or by
        removing them and the RUN completely ("purge"). Requires
        ``replace_run`` to be `True`.
    data_query : `str`
        User query selection expression.
    data_id_table : `~collections.abc.Iterable` [convertible to \
            `lsst.resources.ResourcePath`]
        Paths to data ID tables to join in.
    show : `lsst.ctrl.mpexec.showInfo.ShowInfo`
        Descriptions of what to dump to stdout.
    clobber_outputs : `bool`
        Remove outputs from previous execution of the same quantum before new
        execution.  If ``skip_existing`` is also passed, then only failed
        quanta will be clobbered.
    dataset_query_constraint : `str`
        Control constraining graph building using pre-existing dataset types.
        Valid values are off, all, or a comma separated list of dataset type
        names.
    rebase : `bool`
        If `True` then reset output collection chain if it is inconsistent with
        the ``inputs``.
    mock : `bool`
        If True, use a mocked version of the pipeline.
    unmocked_dataset_types : `collections.abc.Sequence` [ `str` ], optional
        List of overall-input dataset types that should not be mocked.
    mock_failure : `~collections.abc.Mapping`
        Quanta that should raise exceptions.
    for_execution : `bool`, optional
        If `True`, the script is being used to feed another that will execute
        the given quanta, and hence all information needed for execution must
        be loaded.
    for_init_output_run : `bool`, optional
        If `True`, the script is being used to feed another that will
        initialize the output run, and hence all information needed to do so
        must be loaded.
    **kwargs : `dict` [`str`, `str`]
        Ignored; click commands may accept options for more than one script
        function and pass all the option kwargs to each of the script functions
        which ignore these unused kwargs.

    Returns
    -------
    qg : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
        The quantum graph object that was created or loaded.
    """
    # make sure that --extend-run always enables --skip-existing
    if extend_run:
        skip_existing = True

    skip_existing_in = tuple(skip_existing_in) if skip_existing_in is not None else ()
    if data_query is None:
        data_query = ""
    inputs = list(ensure_iterable(input)) if input else []
    del input

    butler, collections, run = ButlerFactory.make_butler_and_collections(
        butler_config,
        output=output,
        output_run=output_run,
        inputs=inputs,
        extend_run=extend_run,
        rebase=rebase,
        replace_run=replace_run,
        prune_replaced=prune_replaced,
    )

    if skip_existing and run:
        skip_existing_in += (run,)

    qgc: PredictedQuantumGraphComponents
    if qgraph is not None:
        # click passes empty tuple as default value for qgraph_node_id
        quantum_ids = (
            {uuid.UUID(q) if not isinstance(q, uuid.UUID) else q for q in qgraph_node_id}
            if qgraph_node_id
            else None
        )
        qgraph = ResourcePath(qgraph)
        match qgraph.getExtension():
            case ".qgraph":
                qgc = PredictedQuantumGraphComponents.from_old_quantum_graph(
                    QuantumGraph.loadUri(
                        qgraph,
                        butler.dimensions,
                        nodes=quantum_ids,
                        graphID=BuildId(qgraph_id) if qgraph_id is not None else None,
                    )
                )
            case ".qg":
                if qgraph_id is not None:
                    _LOG.warning("--qgraph-id is ignored when loading new '.qg' files.")
                if for_execution or for_init_output_run or save_qgraph or show.needs_full_qg:
                    import_mode = TaskImportMode.ASSUME_CONSISTENT_EDGES
                else:
                    import_mode = TaskImportMode.DO_NOT_IMPORT
                with PredictedQuantumGraph.open(qgraph, import_mode=import_mode) as reader:
                    if for_execution or qgraph_dot or qgraph_mermaid or show.needs_full_qg or qgraph_node_id:
                        # This reads everything for the given quanta.
                        reader.read_execution_quanta(quantum_ids)
                    elif for_init_output_run:
                        reader.read_init_quanta()
                    else:
                        reader.read_thin_graph()
                    qgc = reader.components
            case ext:
                raise ValueError(f"Unrecognized extension for quantum graph: {ext!r}")

        # pipeline can not be provided in this case
        if pipeline_graph_factory:
            raise ValueError(
                "Pipeline must not be given when quantum graph is read from "
                f"file: {bool(pipeline_graph_factory)}"
            )
    else:
        if pipeline_graph_factory is None:
            raise ValueError("Pipeline must be given when quantum graph is not read from file.")
        # We can't resolve the pipeline graph if we're mocking until after
        # we've done the mocking (and the QG build will resolve on its own
        # anyway).
        pipeline_graph = pipeline_graph_factory(resolve=False)
        if mock:
            from lsst.pipe.base.tests.mocks import mock_pipeline_graph

            pipeline_graph = mock_pipeline_graph(
                pipeline_graph,
                unmocked_dataset_types=unmocked_dataset_types,
                force_failures=mock_failure,
            )
        data_id_tables = []
        for table_file in data_id_table:
            with ResourcePath(table_file).as_local() as local_path:
                table = Table.read(local_path.ospath)
                # Add the filename to the metadata for more logging
                # information down in the QG builder.
                table.meta["filename"] = table_file
                data_id_tables.append(table)
        # make execution plan (a.k.a. DAG) for pipeline
        graph_builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            butler,
            where=data_query,
            skip_existing_in=skip_existing_in,
            clobber=clobber_outputs,
            dataset_query_constraint=DatasetQueryConstraintVariant.fromExpression(dataset_query_constraint),
            input_collections=collections,
            output_run=run,
            data_id_tables=data_id_tables,
        )
        # Accumulate metadata (QB builder adds some of its own).
        metadata = {
            "butler_argument": str(butler_config),
            "extend_run": extend_run,
            "skip_existing_in": skip_existing_in,
            "skip_existing": skip_existing,
            "data_query": data_query,
        }
        assert run is not None, "Butler output run collection must be defined"
        qgc = graph_builder.finish(
            output, metadata=metadata, attach_datastore_records=qgraph_datastore_records
        )

    if save_qgraph:
        _LOG.verbose("Writing quantum graph to %r.", save_qgraph)
        qgc.write(save_qgraph)

    qg = qgc.assemble()

    if not summarize_quantum_graph(qg):
        return None

    if qgraph_dot:
        _LOG.verbose("Writing quantum graph DOT visualization to %r.", qgraph_dot)
        graph2dot(qg, qgraph_dot)

    if qgraph_mermaid:
        _LOG.verbose("Writing quantum graph Mermaid visualization to %r.", qgraph_mermaid)
        graph2mermaid(qg, qgraph_mermaid)

    # optionally dump some info.
    show.show_graph_info(qg, butler_config)

    return qg
