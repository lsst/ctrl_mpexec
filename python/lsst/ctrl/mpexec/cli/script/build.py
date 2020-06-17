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

from click import ClickException

from lsst.daf.butler.cli.cliLog import CliLog
from ... import CmdLineFwk


def build(order_pipeline=None, pipeline=None, pipeline_actions=(), pipeline_dot=None, save_pipeline=None,
          show=(), log_level=None):
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
    pipeline_actions : `list` [`PipelineAction`]]
        A list of pipeline actions in the order they should be executed.
    pipeline_dot : `str`
        Path location for storing GraphViz DOT representation of a pipeline.
    pipeline : `str`
        Path location of a pipeline definition file in YAML format.
    save_pipeline : `str`
        Path location for storing resulting pipeline definition in YAML format.
    show : `list` [`str`]
        Descriptions of what to dump to stdout.

    Returns
    -------
    pipeline : `lsst.pipe.base.Pipeline`
        The pipeline instance that was built.
    """
    if log_level is not None:
        CliLog.setLogLevels(log_level)

    class MakePipelineArgs:
        """A container class for arguments to CmdLineFwk.makePipeline, whose
        API (currently) is written to accept inputs from argparse in a generic
        container class.
        """
        def __init__(self, pipeline, pipeline_actions, pipeline_dot, save_pipeline):
            self.pipeline = pipeline
            self.pipeline_dot = pipeline_dot
            self.save_pipeline = save_pipeline
            self.pipeline_actions = pipeline_actions

    args = MakePipelineArgs(pipeline, pipeline_actions, pipeline_dot, save_pipeline)

    f = CmdLineFwk()
    try:
        pipeline = f.makePipeline(args)
    except Exception as exc:
        raise ClickException(f"Failed to build pipeline: {exc}") from exc

    class ShowInfoArgs:
        """A container class for arguments to CmdLineFwk.showInfo, whose
        API (currently) is written to accept inputs from argparse in a generic
        container class.
        """

        def __init__(self, show):
            self.show = show

    args = ShowInfoArgs(show)
    f.showInfo(args, pipeline)

    return pipeline
