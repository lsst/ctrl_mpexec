# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

from ... import CmdLineFwk

_log = logging.getLogger(__name__.partition(".")[2])


def qgraph(pipelineObj, qgraph, qgraph_id, qgraph_node_id, skip_existing, save_qgraph, save_single_quanta,
           qgraph_dot, butler_config, input, output, output_run, extend_run, replace_run, prune_replaced,
           data_query, show, **kwargs):
    """Implements the command line interface `pipetask qgraph` subcommand,
    should only be called by command line tools and unit test code that test
    this function.

    Parameters
    ----------
    pipelineObj : `pipe.base.Pipeline` or None.
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
    skip_existing : `bool`
        If all Quantum outputs already exist in the output RUN collection then
        that Quantum will be excluded from the QuantumGraph. Will only be used
        if `extend_run` flag is set.
    save_qgraph : `str` or `None`
        URI location for storing a serialized quantum graph definition as a
        pickle file.
    save_single_quanta : `str` or `None`
        Format string of URI locations for storing individual quantum graph
        definition (pickle files). The curly brace {} in the input string will
        be replaced by a quantum number.
    qgraph_dot : `str` or `None`
        Path location for storing GraphViz DOT representation of a quantum graph.
    butler_config : `str`, `dict`, or `lsst.daf.butler.Config`
        If `str`, `butler_config` is the path location of the gen3
        butler/registry config file. If `dict`, `butler_config` is key value
        pairs used to init or update the `lsst.daf.butler.Config` instance. If
        `Config`, it is the object used to configure a Butler.
    input : `str`
        Comma-separated names of the input collection(s). Entries may include a
        colon (:), the first string is a dataset type name that restricts the
        search in that collection.
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
    prune_replaced : "unstore", "purge", or `None`.
        If not `None`, delete the datasets in the collection replaced by
        `replace_run`, either just from the datastore ("unstore") or by
        removing them and the RUN completely ("purge"). Requires `replace_run`.
    data_query : `str`
        User query selection expression.
    show : `list` [`str`] or `None`
        Descriptions of what to dump to stdout.
    kwargs : `dict` [`str`, `str`]
        Ignored; click commands may accept options for more than one script
        function and pass all the option kwargs to each of the script functions
        which ingore these unused kwargs.

    Returns
    -------
    qgraph : `lsst.pipe.base.QuantumGraph`
        The qgraph object that was created.
    """
    args = SimpleNamespace(qgraph=qgraph,
                           qgraph_id=qgraph_id,
                           qgraph_node_id=qgraph_node_id,
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
                           show=show,
                           skip_existing=skip_existing)

    f = CmdLineFwk()
    qgraph = f.makeGraph(pipelineObj, args)

    # optionally dump some info.
    if show:
        f.showInfo(args, pipelineObj, qgraph)

    return qgraph
