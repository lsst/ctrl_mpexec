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

from __future__ import annotations

__all__ = ["ExecutionStatus", "Report", "QuantumReport"]

import enum
import sys
from typing import Dict, List, Optional

from lsst.daf.butler import DataCoordinate, DataId, DataIdValue
from lsst.utils.introspection import get_full_type_name
from pydantic import BaseModel, validator


def _serializeDataId(dataId: DataId) -> Dict[str, DataIdValue]:
    if isinstance(dataId, DataCoordinate):
        return dataId.byName()
    else:
        return dataId  # type: ignore


class ExecutionStatus(enum.Enum):
    """Possible values for job execution status.

    Status `FAILURE` is set if one or more tasks failed. Status `TIMEOUT` is
    set if there are no failures but one or more tasks timed out. Timeouts can
    only be detected in multi-process mode, child task is killed on timeout
    and usually should have non-zero exit code.
    """

    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"


class ExceptionInfo(BaseModel):
    """Information about exception."""

    className: str
    """Name of the exception class if exception was raised."""

    message: str
    """Exception message for in-process quantum execution, None if
    quantum was executed in sub-process.
    """

    @classmethod
    def from_exception(cls, exception: Exception) -> ExceptionInfo:
        """Construct instance from an exception."""
        return cls(className=get_full_type_name(exception), message=str(exception))


class QuantumReport(BaseModel):
    """Task execution report for a single Quantum."""

    status: ExecutionStatus = ExecutionStatus.SUCCESS
    """Execution status, one of the values in `ExecutionStatus` enum."""

    dataId: Dict[str, DataIdValue]
    """Quantum DataId."""

    taskLabel: Optional[str]
    """Label for a task executing this Quantum."""

    exitCode: Optional[int] = None
    """Exit code for a sub-process executing Quantum, None for in-process
    Quantum execution. Negative if process was killed by a signal.
    """

    exceptionInfo: Optional[ExceptionInfo] = None
    """Exception information if exception was raised."""

    def __init__(
        self,
        dataId: DataId,
        taskLabel: str,
        status: ExecutionStatus = ExecutionStatus.SUCCESS,
        exitCode: Optional[int] = None,
        exceptionInfo: Optional[ExceptionInfo] = None,
    ):
        super().__init__(
            status=status,
            dataId=_serializeDataId(dataId),
            taskLabel=taskLabel,
            exitCode=exitCode,
            exceptionInfo=exceptionInfo,
        )

    @classmethod
    def from_exception(
        cls,
        exception: Exception,
        dataId: DataId,
        taskLabel: str,
    ) -> QuantumReport:
        """Construct report instance from an exception and other pieces of
        data.
        """
        return cls(
            status=ExecutionStatus.FAILURE,
            dataId=dataId,
            taskLabel=taskLabel,
            exceptionInfo=ExceptionInfo.from_exception(exception),
        )

    @classmethod
    def from_exit_code(
        cls,
        exitCode: int,
        dataId: DataId,
        taskLabel: str,
    ) -> QuantumReport:
        """Construct report instance from an exit code and other pieces of
        data.
        """
        return cls(
            status=ExecutionStatus.SUCCESS if exitCode == 0 else ExecutionStatus.FAILURE,
            dataId=dataId,
            taskLabel=taskLabel,
            exitCode=exitCode,
        )


class Report(BaseModel):
    """Execution report for the whole job with one or few quanta."""

    status: ExecutionStatus = ExecutionStatus.SUCCESS
    """Job status."""

    cmdLine: Optional[List[str]] = None
    """Command line for the whole job."""

    exitCode: Optional[int] = None
    """Job exit code, this obviously cannot be set in pipetask."""

    exceptionInfo: Optional[ExceptionInfo] = None
    """Exception information if exception was raised."""

    quantaReports: List[QuantumReport] = []
    """List of per-quantum reports, ordering is not specified. Some or all
    quanta may not produce a report.
    """

    @validator("cmdLine", always=True)
    def _set_cmdLine(cls, v: Optional[List[str]]) -> List[str]:  # noqa: N805
        if v is None:
            v = sys.argv
        return v

    def set_exception(self, exception: Exception) -> None:
        """Update exception information from an exception object."""
        self.exceptionInfo = ExceptionInfo.from_exception(exception)
