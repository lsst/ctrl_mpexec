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

import click

from lsst.daf.butler.cli.butler import LoaderCLI
from lsst.daf.butler.cli.opt import log_level_option, long_log_option
from lsst.daf.butler.cli.utils import ForwardOptions


class PipetaskCLI(LoaderCLI):

    localCmdPkg = "lsst.ctrl.mpexec.cli.cmd"


@click.command(cls=PipetaskCLI, chain=True, context_settings=dict(help_option_names=["-h", "--help"]))
@log_level_option()
@long_log_option()
def cli(log_level, long_log):
    # log_level is handled by get_command and list_commands, and is called in
    # one of those functions before this is called.
    pass


class PipetaskObjects:
    def __init__(self):

        # The pipeline object that gets created by a subcommand
        self.pipeline = None

        # The qgraph object that gets created by a subcommand
        self.qgraph = None

        # The arguments used to initialize a butler in a subcommand, can be
        # used by subsequent subcommands.
        self.butlerArgs = ForwardOptions()


@cli.resultcallback()
def processPipeline(processors, log_level, long_log):
    objs = PipetaskObjects()
    for processor in processors:
        objs = processor(objs)


def main():
    return cli()
