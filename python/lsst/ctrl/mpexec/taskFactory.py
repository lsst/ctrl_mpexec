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

__all__ = ["TaskFactory"]

import logging
from typing import TYPE_CHECKING, Optional

from lsst.daf.butler import DatasetType
from lsst.pipe.base import TaskFactory as BaseTaskFactory

if TYPE_CHECKING:
    from lsst.daf.butler import Butler
    from lsst.pex.config import Config
    from lsst.pipe.base import PipelineTask
    from lsst.pipe.base.configOverrides import ConfigOverrides

_LOG = logging.getLogger(__name__)


class TaskFactory(BaseTaskFactory):
    """Class instantiating PipelineTasks."""

    def makeTask(
        self,
        taskClass: type[PipelineTask],
        label: Optional[str],
        config: Optional[Config],
        overrides: Optional[ConfigOverrides],
        butler: Optional[Butler],
    ) -> PipelineTask:
        """Create new PipelineTask instance from its class.

        Parameters
        ----------
        taskClass : type
            PipelineTask class.
        label : `str` or `None`
            The label of the new task; if `None` then use
            ``taskClass._DefaultName``.
        config : `pex.Config` or None
            Configuration object, if ``None`` then use task-defined
            configuration class to create new instance.
        overrides : `ConfigOverrides` or None
            Configuration overrides, this should contain all overrides to be
            applied to a default task config, including instrument-specific,
            obs-package specific, and possibly command-line overrides.
        butler : `lsst.daf.butler.Butler` or None
            Butler instance used to obtain initialization inputs for
            PipelineTasks.  If None, some PipelineTasks will not be usable

        Returns
        -------
        Instance of a PipelineTask class or None on errors.

        Raises
        ------
        Any exceptions that are raised by PipelineTask constructor or its
        configuration class are propagated back to caller.
        """

        # configuration
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        elif overrides is not None:
            _LOG.warning(
                "Both config and overrides are specified for task %s, overrides are ignored",
                taskClass.__name__,
            )

        # if we don't have a butler, try to construct without initInputs;
        # let PipelineTasks raise if that's impossible
        if butler is None:
            initInputs = None
        else:
            connections = config.connections.ConnectionsClass(config=config)
            descriptorMap = {}
            for name in connections.initInputs:
                attribute = getattr(connections, name)
                dsType = DatasetType(
                    attribute.name, butler.registry.dimensions.extract(set()), attribute.storageClass
                )
                descriptorMap[name] = dsType
            initInputs = {k: butler.get(v) for k, v in descriptorMap.items()}

        # Freeze the config
        config.freeze()

        # make task instance
        task = taskClass(config=config, initInputs=initInputs, name=label)

        return task
