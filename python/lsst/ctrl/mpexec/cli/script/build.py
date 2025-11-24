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

__all__ = ("build",)

from lsst.daf.butler import Butler
from lsst.pipe.base import Pipeline
from lsst.pipe.base.pipeline_graph import visualization
from lsst.resources import ResourcePathExpression

from ..._pipeline_graph_factory import PipelineGraphFactory
from ...showInfo import ShowInfo
from ..utils import _PipelineAction


def build(
    *,
    pipeline: ResourcePathExpression | Pipeline,
    pipeline_actions: list[_PipelineAction] | _PipelineAction,
    pipeline_dot: str,
    pipeline_mermaid: str,
    save_pipeline: str,
    show: ShowInfo,
    butler_config: ResourcePathExpression | None = None,
    select_tasks: str = "",
    **kwargs: object,
) -> PipelineGraphFactory:
    """Implement the command line interface `pipetask build` subcommand.

    Should only be called by command line tools and unit test code that tests
    this function.

    Build and optionally save pipeline definition.

    Returns the pipeline instance that was built, for testing and for using
    this function with other script functions.

    Parameters
    ----------
    pipeline : `str` or `lsst.pipe.base.Pipeline`
        Path location of a pipeline definition file in YAML format.
    pipeline_actions : `list` [`PipelineAction`] or `PipelineAction`
        A list of pipeline actions in the order they should be executed.
    pipeline_dot : `str`
        Path location for storing GraphViz DOT representation of a pipeline.
    pipeline_mermaid : `str`
        Path location for storing Mermaid representation of a pipeline.
    save_pipeline : `str`
        Path location for storing resulting pipeline definition in YAML format.
    show : `lsst.ctrl.mpexec.showInfo.ShowInfo`
        Descriptions of what to dump to stdout.
    butler_config : `str`, `dict`, or `lsst.daf.butler.Config`, optional
        If `str`, `butler_config` is the path location of the gen3
        butler/registry config file. If `dict`, `butler_config` is key value
        pairs used to init or update the `lsst.daf.butler.Config` instance. If
        `Config`, it is the object used to configure a Butler.
        Only used to resolve pipeline graphs for --show pipeline-graph and
        --show task-graph.
    select_tasks : `str`, optional
        String expression that filters the tasks in the pipeline.
    **kwargs
        Ignored; click commands may accept options for more than one script
        function and pass all the option kwargs to each of the script functions
        which ignore these unused kwargs.

    Returns
    -------
    pipeline_graph_factory : `..PipelineGraphFactory`
        A helper object that holds the built pipeline and can turn it into a
        pipeline graph.

    Raises
    ------
    Exception
        Raised if there is a failure building the pipeline.
    """
    # If pipeline_actions is a single instance, not a list, then put it in
    # a list. _PipelineAction is a namedtuple, so we can't use
    # `lsst.utils.iteration.iterable` because a namedtuple *is* iterable,
    # but we need a list of _PipelineAction.
    if isinstance(pipeline_actions, _PipelineAction):
        pipeline_actions = [pipeline_actions]

    if pipeline:
        if not isinstance(pipeline, Pipeline):
            pipeline = Pipeline.from_uri(pipeline)
    else:
        pipeline = Pipeline("anonymous")

    # loop over all pipeline actions and apply them in order
    for action in pipeline_actions:
        match action.action:
            case "add_instrument":
                pipeline.addInstrument(action.value)
            case "new_task":
                pipeline.addTask(action.value, action.label)
            case "delete_task":
                pipeline.removeTask(action.label)
            case "config":
                # action value string is "field=value", split it at '='
                field, _, value = action.value.partition("=")
                pipeline.addConfigOverride(action.label, field, value)
            case "configfile":
                pipeline.addConfigFile(action.label, action.value)
            case _:
                raise ValueError(f"Unexpected pipeline action: {action.action}")

    if save_pipeline:
        pipeline.write_to_uri(save_pipeline)

    butler: Butler | None = None
    if butler_config:
        butler = Butler.from_config(butler_config, writeable=False)

    try:
        pipeline_graph_factory = PipelineGraphFactory(pipeline, butler, select_tasks)
    finally:
        if butler is not None:
            butler.close()

    if pipeline_dot:
        with open(pipeline_dot, "w") as stream:
            visualization.show_dot(
                pipeline_graph_factory(visualization_only=True),
                stream,
                dataset_types=True,
                task_classes="full",
            )

    if pipeline_mermaid:
        # Determine output format based on file extension.
        if pipeline_mermaid.endswith(".svg"):
            output_format = "svg"
            file_mode = "wb"
        elif pipeline_mermaid.endswith(".png"):
            output_format = "png"
            file_mode = "wb"
        else:  # Default to the text-based mmd format.
            output_format = "mmd"
            file_mode = "w"

        with open(pipeline_mermaid, file_mode) as stream:
            visualization.show_mermaid(
                pipeline_graph_factory(visualization_only=True),
                stream,
                output_format=output_format,
                width=4500 if output_format != "mmd" else None,
                dataset_types=True,
                task_classes="full",
            )

    show.show_pipeline_info(pipeline_graph_factory)

    return pipeline_graph_factory
