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


import collections
import contextlib
import re

from lsst.daf.butler.cli.opt import config_file_option, config_option
from lsst.daf.butler.cli.utils import MWCommand, split_commas
from lsst.pipe.base.cli.opt import instrument_option

from .opt import delete_option, task_option

# Class which determines an action that needs to be performed
# when building pipeline, its attributes are:
#   action: the name of the action, e.g. "new_task", "delete_task"
#   label:  task label, can be None if action does not require label
#   value:  argument value excluding task label.
_PipelineAction = collections.namedtuple("_PipelineAction", "action,label,value")


class _PipelineActionType:
    """Class defining a callable type which converts strings into
    ``_PipelineAction`` instances.

    Parameters
    ----------
    action : `str`
        Name of the action, will become `action` attribute of instance.
    regex : `str`
        Regular expression for argument value, it can define groups 'label'
        and 'value' which will become corresponding attributes of a
        returned instance.
    """

    def __init__(self, action: str, regex: str = ".*", valueType: type = str):
        self.action = action
        self.regex = re.compile(regex)
        self.valueType = valueType

    def __call__(self, value: str) -> _PipelineAction:
        match = self.regex.match(value)
        if not match:
            raise TypeError(
                f"Unrecognized syntax for option {self.action!r}: {value!r} "
                f"(does not match pattern {self.regex.pattern})"
            )
        # get "label" group or use None as label
        try:
            label = match.group("label")
        except IndexError:
            label = None
        # if "value" group is not defined use whole string
        with contextlib.suppress(IndexError):
            value = match.group("value")

        value = self.valueType(value)
        return _PipelineAction(self.action, label, value)

    def __repr__(self) -> str:
        """Return string representation of this class."""
        return f"_PipelineActionType(action={self.action})"


_ACTION_ADD_TASK = _PipelineActionType("new_task", "(?P<value>[^:]+)(:(?P<label>.+))?")
_ACTION_DELETE_TASK = _PipelineActionType("delete_task", "(?P<value>)(?P<label>.+)")
_ACTION_CONFIG = _PipelineActionType("config", "(?P<label>.+):(?P<value>.+=.+)")
_ACTION_CONFIG_FILE = _PipelineActionType("configfile", "(?P<label>.+):(?P<value>.+)")
_ACTION_ADD_INSTRUMENT = _PipelineActionType("add_instrument", "(?P<value>[^:]+)")


def makePipelineActions(
    args: list[str],
    taskFlags: list[str] = task_option.opts(),
    deleteFlags: list[str] = delete_option.opts(),
    configFlags: list[str] = config_option.opts(),
    configFileFlags: list[str] = config_file_option.opts(),
    instrumentFlags: list[str] = instrument_option.opts(),
) -> list[_PipelineAction]:
    """Make a list of pipeline actions from a list of option flags and
    values.

    Parameters
    ----------
    args : `list` [`str`]
        The arguments, option flags, and option values in the order they were
        passed in on the command line.
    taskFlags : `list` [`str`], optional
        The option flags to use to recognize a task action, by default
        task_option.opts()
    deleteFlags : `list` [`str`], optional
        The option flags to use to recognize a delete action, by default
        delete_option.opts()
    configFlags : `list` [`str`], optional
        The option flags to use to recognize a config action, by default
        config_option.opts()
    configFileFlags : `list` [`str`], optional
        The option flags to use to recognize a config-file action, by default
        config_file_option.opts()
    instrumentFlags : `list` [`str`], optional
        The option flags to use to recognize an instrument action, by default
        instrument_option.opts()

    Returns
    -------
    pipelineActions : `list` [`_PipelineActionType`]
        A list of pipeline actions constructed form their arguments in args,
        in the order they appeared in args.
    """
    pipelineActions = []
    # iterate up to the second-to-last element, if the second to last element
    # is a key we're looking for, the last item will be its value.
    for i in range(len(args) - 1):
        if args[i] in taskFlags:
            pipelineActions.append(_ACTION_ADD_TASK(args[i + 1]))
        elif args[i] in deleteFlags:
            pipelineActions.append(_ACTION_DELETE_TASK(args[i + 1]))
        elif args[i] in configFlags:
            pipelineActions.append(_ACTION_CONFIG(args[i + 1]))
        elif args[i] in configFileFlags:
            # --config-file allows multiple comma-separated values.
            configfile_args = split_commas(None, None, args[i + 1])
            pipelineActions.extend(_ACTION_CONFIG_FILE(c) for c in configfile_args)
        elif args[i] in instrumentFlags:
            pipelineActions.append(_ACTION_ADD_INSTRUMENT(args[i + 1]))
    return pipelineActions


class PipetaskCommand(MWCommand):
    """Command subclass with pipetask-command specific overrides."""

    extra_epilog = "See 'pipetask --help' for more options."
