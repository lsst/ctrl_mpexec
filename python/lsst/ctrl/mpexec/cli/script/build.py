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

from types import SimpleNamespace

from ... import CmdLineFwk
from ..utils import _PipelineAction


def build(  # type: ignore
    order_pipeline, pipeline, pipeline_actions, pipeline_dot, save_pipeline, show, **kwargs
):
    """Implements the command line interface `pipetask build` subcommand,
    should only be called by command line tools and unit test code that tests
    this function.

    Build and optionally save pipeline definition.

    Returns the pipeline instance that was built, for testing and for using
    this function with other script functions.

    Parameters
    ----------
    order_pipeline : `bool`
        If true, order tasks in pipeline based on their data dependencies,
        ordering is performed as last step before saving or executing pipeline.
    pipeline_actions : `list` [`PipelineAction`]] or `PipelineAction`
        A list of pipeline actions in the order they should be executed.
    pipeline_dot : `str`
        Path location for storing GraphViz DOT representation of a pipeline.
    pipeline : `str`
        Path location of a pipeline definition file in YAML format.
    save_pipeline : `str`
        Path location for storing resulting pipeline definition in YAML format.
    show : `lsst.ctrl.mpexec.showInfo.ShowInfo`
        Descriptions of what to dump to stdout.
    kwargs : `dict` [`str`, `str`]
        Ignored; click commands may accept options for more than one script
        function and pass all the option kwargs to each of the script functions
        which ingore these unused kwargs.

    Returns
    -------
    pipeline : `lsst.pipe.base.Pipeline`
        The pipeline instance that was built.

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
        pipeline_actions = (pipeline_actions,)

    args = SimpleNamespace(
        pipeline=pipeline,
        pipeline_actions=pipeline_actions,
        pipeline_dot=pipeline_dot,
        save_pipeline=save_pipeline,
    )

    f = CmdLineFwk()

    # Will raise an exception if it fails to build the pipeline.
    pipeline = f.makePipeline(args)

    show.show_pipeline_info(pipeline)

    return pipeline
