# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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

from lsst.daf.butler.cli.utils import MWOption


class save_pipeline_option:  # noqa: N801

    defaultHelp = "Location for storing resulting pipeline definition in YAML format."

    def __init__(self, required=False, help=defaultHelp):
        self.help = help
        self.required = required
        self.type = click.Path(dir_okay=False, file_okay=True, writable=True)

    def __call__(self, f):
        return click.option("-s", "--save-pipeline", cls=MWOption,
                            help=self.help,
                            required=self.required,
                            type=self.type)(f)
