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

from ... import CmdLineFwk, TaskFactory


def run_qbb(
    butler_config: str,
    qgraph: str,
    config_search_path: list[str] | None,
    qgraph_id: str | None,
    qgraph_node_id: list[int] | None,
    processes: int,
    pdb: str | None,
    profile: str | None,
    debug: bool,
    start_method: str | None,
    timeout: int | None,
    fail_fast: bool,
    summary: str | None,
    enable_implicit_threading: bool,
) -> None:
    """Implements the command line interface `pipetask run-qbb` subcommand,
    should only be called by command line tools and unit test code that tests
    this function.

    Parameters
    ----------
    butler_config : `str`
        The path location of the gen3 butler/registry config file.
    qgraph : `str`
        URI location for a serialized quantum graph definition.
    config_search_path : `list` [`str`]
        Additional search paths for butler configuration.
    qgraph_id : `str` or `None`
        Quantum graph identifier, if specified must match the identifier of the
        graph loaded from a file. Ignored if graph is not loaded from a file.
    qgraph_node_id : `iterable` of `int`, or `None`
        Only load a specified set of nodes if graph is loaded from a file,
        nodes are identified by integer IDs.
    processes : `int`
        The number of processes to use.
    pdb : `str` or `None`
        Debugger to launch for exceptions.
    profile : `str`
        File name to dump cProfile information to.
    debug : `bool`
        If true, enable debugging output using lsstDebug facility (imports
        debug.py).
    start_method : `str` or `None`
        Start method from `multiprocessing` module, `None` selects the best
        one for current platform.
    timeout : `int`
        Timeout for multiprocessing; maximum wall time (sec).
    fail_fast : `bool`
        If true then stop processing at first error, otherwise process as many
        tasks as possible.
    summary : `str` or `None`
        File path to store job report in JSON format.
    enable_implicit_threading : `bool`
        If `True`, do not disable implicit threading by third-party libraries.
        Implicit threading is always disabled during actual quantum execution
        if ``processes > 1``.
    """
    args = SimpleNamespace(
        butler_config=butler_config,
        qgraph=qgraph,
        config_search_path=config_search_path,
        qgraph_id=qgraph_id,
        qgraph_node_id=qgraph_node_id,
        processes=processes,
        pdb=pdb,
        profile=profile,
        enableLsstDebug=debug,
        start_method=start_method,
        timeout=timeout,
        fail_fast=fail_fast,
        summary=summary,
        enable_implicit_threading=enable_implicit_threading,
    )

    f = CmdLineFwk()
    task_factory = TaskFactory()
    f.runGraphQBB(task_factory, args)
