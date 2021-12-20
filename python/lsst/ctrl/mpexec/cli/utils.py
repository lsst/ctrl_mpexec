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


import collections
import re

from lsst.daf.butler.cli.opt import config_file_option, config_option
from lsst.daf.butler.cli.utils import MWCommand
from lsst.obs.base.cli.opt import instrument_option

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

    def __init__(self, action, regex=".*", valueType=str):
        self.action = action
        self.regex = re.compile(regex)
        self.valueType = valueType

    def __call__(self, value):
        match = self.regex.match(value)
        if not match:
            raise TypeError("Unrecognized option syntax: " + value)
        # get "label" group or use None as label
        try:
            label = match.group("label")
        except IndexError:
            label = None
        # if "value" group is not defined use whole string
        try:
            value = match.group("value")
        except IndexError:
            pass
        value = self.valueType(value)
        return _PipelineAction(self.action, label, value)

    def __repr__(self):
        """String representation of this class."""
        return f"_PipelineActionType(action={self.action})"


_ACTION_ADD_TASK = _PipelineActionType("new_task", "(?P<value>[^:]+)(:(?P<label>.+))?")
_ACTION_DELETE_TASK = _PipelineActionType("delete_task", "(?P<value>)(?P<label>.+)")
_ACTION_CONFIG = _PipelineActionType("config", "(?P<label>.+):(?P<value>.+=.+)")
_ACTION_CONFIG_FILE = _PipelineActionType("configfile", "(?P<label>.+):(?P<value>.+)")
_ACTION_ADD_INSTRUMENT = _PipelineActionType("add_instrument", "(?P<value>[^:]+)")


def makePipelineActions(
    args,
    taskFlags=task_option.opts(),
    deleteFlags=delete_option.opts(),
    configFlags=config_option.opts(),
    configFileFlags=config_file_option.opts(),
    instrumentFlags=instrument_option.opts(),
):
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
            pipelineActions.append(_ACTION_CONFIG_FILE(args[i + 1]))
        elif args[i] in instrumentFlags:
            pipelineActions.append(_ACTION_ADD_INSTRUMENT(args[i + 1]))
    return pipelineActions


class PipetaskCommand(MWCommand):
    """Command subclass with pipetask-command specific overrides."""

    extra_epilog = "See 'pipetask --help' for more options."
