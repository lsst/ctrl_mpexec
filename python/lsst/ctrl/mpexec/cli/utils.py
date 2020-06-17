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


from lsst.daf.butler.cli.opt import (config_file_option, config_option)
from lsst.obs.base.cli.opt import instrument_parameter
from .opt import (delete_option, task_option)
from ..cmdLineParser import (_ACTION_ADD_TASK,
                             _ACTION_DELETE_TASK,
                             _ACTION_CONFIG,
                             _ACTION_CONFIG_FILE,
                             _ACTION_ADD_INSTRUMENT)


def makePipelineActions(args,
                        taskFlags=task_option.optionFlags,
                        deleteFlags=delete_option.optionFlags,
                        configFlags=config_option.optionFlags,
                        configFileFlags=config_file_option.optionFlags,
                        instrumentFlags=instrument_parameter.optionFlags):
    """Make a list of pipline actions from a list of option flags and
    values.

    Parameters
    ----------
    args : `list` [`str`]
        The arguments, option flags, and option values in the order they were
        passed in on the command line.
    taskFlags : `list` [`str`], optional
        The option flags to use to recoginze a task action, by default
        task_option.optionFlags
    deleteFlags : `list` [`str`], optional
        The option flags to use to recoginze a delete action, by default
        delete_option.optionFlags
    configFlags : `list` [`str`], optional
        The option flags to use to recoginze a config action, by default
        config_option.optionFlags
    configFileFlags : `list` [`str`], optional
        The option flags to use to recoginze a config-file action, by default
        config_file_option.optionFlags
    instrumentFlags : `list` [`str`], optional
        The option flags to use to recoginze an instrument action, by default
        instrument_parameter.optionFlags

    Returns
    -------
    pipelineActions : `list` [`_PipelineActionType`]
        A list of pipeline actions constructed form their arguments in args,
        in the order they appeared in args.
    """
    pipelineActions = []
    # iterate up to the second-to-last element, if the second to last element
    # is a key we're looking for, the last item will be its value.
    for i in range(len(args)-1):
        if args[i] in taskFlags:
            pipelineActions.append(_ACTION_ADD_TASK(args[i+1]))
        elif args[i] in deleteFlags:
            pipelineActions.append(_ACTION_DELETE_TASK(args[i+1]))
        elif args[i] in configFlags:
            pipelineActions.append(_ACTION_CONFIG(args[i+1]))
        elif args[i] in configFileFlags:
            pipelineActions.append(_ACTION_CONFIG_FILE(args[i+1]))
        elif args[i] in instrumentFlags:
            pipelineActions.append(_ACTION_ADD_INSTRUMENT(args[i+1]))
    return pipelineActions
