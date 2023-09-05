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

__all__ = ["TaskFactory"]

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from lsst.pipe.base import TaskFactory as BaseTaskFactory

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef, LimitedButler
    from lsst.pipe.base import PipelineTask, TaskDef

_LOG = logging.getLogger(__name__)


class TaskFactory(BaseTaskFactory):
    """Class instantiating PipelineTasks."""

    def makeTask(
        self, taskDef: TaskDef, butler: LimitedButler, initInputRefs: Iterable[DatasetRef] | None
    ) -> PipelineTask:
        # docstring inherited

        config = taskDef.config

        # Get init inputs from butler.
        init_inputs: dict[str, Any] = {}
        if initInputRefs:
            connections = config.connections.ConnectionsClass(config=config)
            for name in connections.initInputs:
                attribute = getattr(connections, name)
                dataset_type_name = attribute.name
                for ref in initInputRefs:
                    if ref.datasetType.name == dataset_type_name:
                        init_inputs[name] = butler.get(ref)
                        break

        # make task instance
        task = taskDef.taskClass(config=config, initInputs=init_inputs, name=taskDef.label)
        return task
