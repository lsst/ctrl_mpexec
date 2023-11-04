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

__all__ = ["ExceptionInfo", "ExecutionStatus", "Report", "QuantumReport"]

import enum
import sys
from typing import Any

import pydantic
from lsst.daf.butler import DataCoordinate, DataId, DataIdValue
from lsst.daf.butler._compat import PYDANTIC_V2, _BaseModelCompat
from lsst.utils.introspection import get_full_type_name


def _serializeDataId(dataId: DataId) -> dict[str, DataIdValue]:
    if isinstance(dataId, DataCoordinate):
        return dict(dataId.required)
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


class ExceptionInfo(_BaseModelCompat):
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


class QuantumReport(_BaseModelCompat):
    """Task execution report for a single Quantum."""

    status: ExecutionStatus = ExecutionStatus.SUCCESS
    """Execution status, one of the values in `ExecutionStatus` enum."""

    dataId: dict[str, DataIdValue]
    """Quantum DataId."""

    taskLabel: str | None
    """Label for a task executing this Quantum."""

    exitCode: int | None = None
    """Exit code for a sub-process executing Quantum, None for in-process
    Quantum execution. Negative if process was killed by a signal.
    """

    exceptionInfo: ExceptionInfo | None = None
    """Exception information if exception was raised."""

    def __init__(
        self,
        dataId: DataId,
        taskLabel: str,
        status: ExecutionStatus = ExecutionStatus.SUCCESS,
        exitCode: int | None = None,
        exceptionInfo: ExceptionInfo | None = None,
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


class Report(_BaseModelCompat):
    """Execution report for the whole job with one or few quanta."""

    status: ExecutionStatus = ExecutionStatus.SUCCESS
    """Job status."""

    cmdLine: list[str] | None = None
    """Command line for the whole job."""

    exitCode: int | None = None
    """Job exit code, this obviously cannot be set in pipetask."""

    exceptionInfo: ExceptionInfo | None = None
    """Exception information if exception was raised."""

    quantaReports: list[QuantumReport] = []
    """List of per-quantum reports, ordering is not specified. Some or all
    quanta may not produce a report.
    """

    if PYDANTIC_V2:
        # Always want to validate the default value for cmdLine so
        # use a model_validator.
        @pydantic.model_validator(mode="before")  # type: ignore[attr-defined]
        @classmethod
        def _set_cmdLine(cls, data: Any) -> Any:
            if data.get("cmdLine") is None:
                data["cmdLine"] = sys.argv
            return data

    else:

        @pydantic.validator("cmdLine", always=True)
        def _set_cmdLine(cls, v: list[str] | None) -> list[str]:  # noqa: N805
            if v is None:
                v = sys.argv
            return v

    def set_exception(self, exception: Exception) -> None:
        """Update exception information from an exception object."""
        self.exceptionInfo = ExceptionInfo.from_exception(exception)
