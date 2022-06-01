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


from abc import ABC, abstractmethod
from typing import Callable

import click


class ConfirmableResult(ABC):

    """Interface for a results class that can be used by the `confirm`
    function."""

    @abstractmethod
    def describe(self, will: bool) -> str:
        """Get a message describing what will be or was done. This is printed
        just before "Continue?" on the CLI, if confirming, or just before
        "Done." if confirmation is being skipped.

        Parameters
        ----------
        will : bool
            True if confirmation is being requested, False if --no-confirm was
            used, and the action is completed.
        """
        pass

    @abstractmethod
    def on_confirmation(self) -> None:
        """Performs the action that was returned from `describe`. This is
        Called just after the user has confirmed (if needed)."""
        pass

    @property
    @abstractmethod
    def failed(self) -> bool:
        """Query if there was a failure preparing the ConfirmableResult,
        before `on_confirmation` is called."""
        pass

    @property
    @abstractmethod
    def describe_failure(self) -> str:
        """Get a message describing the failure. This is used as the message
        when raising a `ClickException` to stop with exit code 1."""
        pass

    @property
    @abstractmethod
    def can_continue(self) -> bool:
        """Query if the ConfirmableResult can continue. Returns `False` if
        there is no work to be done."""
        pass


def confirm(script_func: Callable[[], ConfirmableResult], confirm: bool) -> ConfirmableResult:
    result = script_func()
    if result.failed:
        raise click.ClickException(result.describe_failure)
    if not result.can_continue:
        print(result.describe(will=True))
        return result
    if confirm:
        print(result.describe(will=True))
        if result.can_continue:
            do_continue = click.confirm("Continue?", default=False)
    else:
        do_continue = True
    if not do_continue:
        print("Aborted.")
    else:
        result.on_confirmation()
        if not confirm:
            print(result.describe(will=False))
        else:
            print("Done.")
    return result
