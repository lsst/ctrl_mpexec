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

__all__ = ("MPGraphExecutor", "MPGraphExecutorError", "MPTimeoutError")

from typing import Literal

from deprecated.sphinx import deprecated

import lsst.pipe.base.mp_graph_executor
from lsst.pipe.base.execution_graph_fixup import ExecutionGraphFixup
from lsst.pipe.base.quantum_graph_executor import QuantumExecutor
from lsst.pipe.base.quantum_reports import Report

# TODO[DM-51962]: Remove this module.


@deprecated(
    "The MPGraphExecutor class has moved to lsst.pipe.base.mp_graph_executor. "
    "This forwarding shim will be removed after v30.",
    version="v30",
    category=FutureWarning,
)
class MPGraphExecutor(lsst.pipe.base.mp_graph_executor.MPGraphExecutor):
    """Implementation of QuantumGraphExecutor using same-host multiprocess
    execution of Quanta.

    This is a deprecated backwards-compatibility shim for
    `lsst.pipe.base.mp_graph_executor.MPGraphExecutor`, which has
    the same functionality with very minor interface changes.

    Parameters
    ----------
    numProc : `int`
        Number of processes to use for executing tasks.
    timeout : `float`
        Time in seconds to wait for tasks to finish.
    quantumExecutor : `QuantumExecutor`
        Executor for single quantum. For multiprocess-style execution when
        ``num_proc`` is greater than one this instance must support pickle.
    startMethod : `str`, optional
        Start method from `multiprocessing` module, `None` selects the best
        one for current platform.
    failFast : `bool`, optional
        If set to ``True`` then stop processing on first error from any task.
    pdb : `str`, optional
        Debugger to import and use (via the ``post_mortem`` function) in the
        event of an exception.
    executionGraphFixup : `.execution_graph_fixup.ExecutionGraphFixup`, \
            optional
        Instance used for modification of execution graph.
    """

    def __init__(
        self,
        numProc: int,
        timeout: float,
        quantumExecutor: QuantumExecutor,
        *,
        startMethod: Literal["spawn"] | Literal["forkserver"] | None = None,
        failFast: bool = False,
        pdb: str | None = None,
        executionGraphFixup: ExecutionGraphFixup | None = None,
    ):
        super().__init__(
            num_proc=numProc,
            timeout=timeout,
            quantum_executor=quantumExecutor,
            start_method=startMethod,
            fail_fast=failFast,
            pdb=pdb,
            execution_graph_fixup=executionGraphFixup,
        )

    @property
    def numProc(self) -> int:
        return self._num_proc

    @property
    def timeout(self) -> float:
        return self._timeout

    @property
    def quantumExecutor(self) -> QuantumExecutor:
        return self._quantum_executor

    @property
    def failFast(self) -> bool:
        return self._fail_fast

    @property
    def pdb(self) -> str | None:
        return self._pdb

    @property
    def executionGraphFixup(self) -> ExecutionGraphFixup | None:
        return self._execution_graph_fixup

    @property
    def report(self) -> Report | None:
        return self._report

    @property
    def startMethod(self) -> str:
        return self._start_method


# We can't make these forwarders warn by subclassing, because an 'except'
# statement on a derived class won't catch a base class instance.

MPGraphExecutorError = lsst.pipe.base.mp_graph_executor.MPGraphExecutorError
MPTimeoutError = lsst.pipe.base.mp_graph_executor.MPTimeoutError
