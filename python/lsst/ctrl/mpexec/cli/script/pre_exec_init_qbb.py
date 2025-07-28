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

from types import SimpleNamespace

from lsst.pipe.base import TaskFactory

from ... import CmdLineFwk


def pre_exec_init_qbb(
    butler_config: str,
    qgraph: str,
    qgraph_id: str | None,
    config_search_path: list[str] | None,
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
    """
    args = SimpleNamespace(
        butler_config=butler_config,
        qgraph=qgraph,
        qgraph_id=qgraph_id,
        config_search_path=config_search_path,
    )

    f = CmdLineFwk()
    task_factory = TaskFactory()
    f.preExecInitQBB(task_factory, args)
