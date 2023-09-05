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

import logging
from types import SimpleNamespace

from lsst.pipe.base.graphBuilder import DatasetQueryConstraintVariant

from ... import CmdLineFwk

_log = logging.getLogger(__name__)


def qgraph(  # type: ignore
    pipelineObj,
    qgraph,
    qgraph_id,
    qgraph_node_id,
    qgraph_datastore_records,
    skip_existing_in,
    skip_existing,
    save_qgraph,
    save_single_quanta,
    qgraph_dot,
    butler_config,
    input,
    output,
    output_run,
    extend_run,
    replace_run,
    prune_replaced,
    data_query,
    show,
    save_execution_butler,
    clobber_execution_butler,
    target_datastore_root,
    transfer,
    clobber_outputs,
    dataset_query_constraint,
    rebase,
    show_qgraph_header=False,
    mock=False,
    unmocked_dataset_types=(),
    mock_failure=(),
    **kwargs,
):
    """Implement the command line interface `pipetask qgraph` subcommand.

    Should only be called by command line tools and unit test code that test
    this function.

    Parameters
    ----------
    pipelineObj : `lsst.pipe.base.Pipeline` or None.
        The pipeline object used to generate a qgraph. If this is not `None`
        then `qgraph` should be `None`.
    qgraph : `str` or `None`
        URI location for a serialized quantum graph definition as a pickle
        file. If this option is not None then `pipeline` should be `None`.
    qgraph_id : `str` or `None`
        Quantum graph identifier, if specified must match the identifier of the
        graph loaded from a file. Ignored if graph is not loaded from a file.
    qgraph_node_id : `list` of `int`, optional
        Only load a specified set of nodes if graph is loaded from a file,
        nodes are identified by integer IDs.
    qgraph_datastore_records : `bool`
        If True then include datastore records into generated quanta.
    skip_existing_in : `list` [ `str` ]
        Accepts list of collections, if all Quantum outputs already exist in
        the specified list of collections then that Quantum will be excluded
        from the QuantumGraph.
    skip_existing : `bool`
        Appends output RUN collection to the ``skip_existing_in`` list.
    save_qgraph : `str` or `None`
        URI location for storing a serialized quantum graph definition as a
        pickle file.
    save_single_quanta : `str` or `None`
        Format string of URI locations for storing individual quantum graph
        definition (pickle files). The curly brace {} in the input string will
        be replaced by a quantum number.
    qgraph_dot : `str` or `None`
        Path location for storing GraphViz DOT representation of a quantum
        graph.
    butler_config : `str`, `dict`, or `lsst.daf.butler.Config`
        If `str`, `butler_config` is the path location of the gen3
        butler/registry config file. If `dict`, `butler_config` is key value
        pairs used to init or update the `lsst.daf.butler.Config` instance. If
        `Config`, it is the object used to configure a Butler.
    input : `list` [ `str` ]
        List of names of the input collection(s).
    output : `str`
        Name of the output CHAINED collection. This may either be an existing
        CHAINED collection to use as both input and output (if `input` is
        `None`), or a new CHAINED collection created to include all inputs
        (if `input` is not `None`). In both cases, the collection's children
        will start with an output RUN collection that directly holds all new
        datasets (see `output_run`).
    output_run : `str`
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
    show : `lsst.ctrl.mpexec.showInfo.ShowInfo`
        Descriptions of what to dump to stdout.
    save_execution_butler : `str` or `None`
        URI location for storing an execution Butler build from the
        QuantumGraph.
    clobber_execution_butler : `bool`
        It True overwrite existing execution butler files if present.
    target_datastore_root : `str` or `None`
        URI location for the execution butler's datastore.
    transfer : `str` or `None`
        Transfer mode for execution butler creation.  This should be a
        ``transfer`` string recognized by
        :func:`lsst.resources.ResourcePath.transfer_from`.
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
    show_qgraph_header : bool, optional
        Controls if the headerData of a QuantumGraph should be printed to the
        terminal. Defaults to False.
    mock : `bool`, optional
        If True, use a mocked version of the pipeline.
    unmocked_dataset_types : `collections.abc.Sequence` [ `str` ], optional
        List of overall-input dataset types that should not be mocked.
    mock_failure : `~collections.abc.Sequence`, optional
        List of quanta that should raise exceptions.
    kwargs : `dict` [`str`, `str`]
        Ignored; click commands may accept options for more than one script
        function and pass all the option kwargs to each of the script functions
        which ignore these unused kwargs.

    Returns
    -------
    qgraph : `lsst.pipe.base.QuantumGraph`
        The qgraph object that was created.
    """
    dataset_query_constraint = DatasetQueryConstraintVariant.fromExpression(dataset_query_constraint)
    args = SimpleNamespace(
        qgraph=qgraph,
        qgraph_id=qgraph_id,
        qgraph_node_id=qgraph_node_id,
        qgraph_datastore_records=qgraph_datastore_records,
        save_qgraph=save_qgraph,
        save_single_quanta=save_single_quanta,
        qgraph_dot=qgraph_dot,
        butler_config=butler_config,
        input=input,
        output=output,
        output_run=output_run,
        extend_run=extend_run,
        replace_run=replace_run,
        prune_replaced=prune_replaced,
        data_query=data_query,
        skip_existing_in=skip_existing_in,
        skip_existing=skip_existing,
        execution_butler_location=save_execution_butler,
        clobber_execution_butler=clobber_execution_butler,
        target_datastore_root=target_datastore_root,
        transfer=transfer,
        clobber_outputs=clobber_outputs,
        dataset_query_constraint=dataset_query_constraint,
        rebase=rebase,
        show_qgraph_header=show_qgraph_header,
        mock=mock,
        unmocked_dataset_types=list(unmocked_dataset_types),
        mock_failure=mock_failure,
    )

    f = CmdLineFwk()
    qgraph = f.makeGraph(pipelineObj, args)

    if qgraph is None:
        return None

    # optionally dump some info.
    show.show_graph_info(qgraph, args)

    return qgraph
