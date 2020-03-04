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

__all__ = ['PreExecInit']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import itertools

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.pipe.base import PipelineDatasetTypes

_LOG = logging.getLogger(__name__.partition(".")[2])


class PreExecInit:
    """Initialization of registry for QuantumGraph execution.

    This class encapsulates all necessary operations that have to be performed
    on butler and registry to prepare them for QuantumGraph execution.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Task factory.
    skipExisting : `bool`, optional
        If `True` then do not try to overwrite any datasets that might exist
        in the butler. If `False` then any existing conflicting dataset will
        cause butler exception.
    """
    def __init__(self, butler, taskFactory, skipExisting=False):
        self.butler = butler
        self.taskFactory = taskFactory
        self.skipExisting = skipExisting

    def initialize(self, graph, saveInitOutputs=True, registerDatasetTypes=False):
        """Perform all initialization steps.

        Convenience method to execute all initialization steps. Instead of
        calling this method and providing all options it is also possible to
        call methods individually.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        saveInitOutputs : `bool`, optional
            If ``True`` (default) then save task "init outputs" to butler.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.
        """
        # register dataset types or check consistency
        self.initializeDatasetTypes(graph, registerDatasetTypes)

        # Save task initialization data or check that saved data
        # is consistent with what tasks would save
        if saveInitOutputs:
            self.saveInitOutputs(graph)

    def initializeDatasetTypes(self, graph, registerDatasetTypes=False):
        """Save or check DatasetTypes output by the tasks in a graph.

        Iterates over all DatasetTypes for all tasks in a graph and either
        tries to add them to registry or compares them to exising ones.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.

        Raises
        ------
        ValueError
            Raised if existing DatasetType is different from DatasetType
            in a graph.
        KeyError
            Raised if ``registerDatasetTypes`` is ``False`` and DatasetType
            does not exist in registry.
        """
        pipeline = list(nodes.taskDef for nodes in graph)
        datasetTypes = PipelineDatasetTypes.fromPipeline(pipeline, registry=self.butler.registry)
        for datasetType in itertools.chain(datasetTypes.initIntermediates, datasetTypes.initOutputs,
                                           datasetTypes.intermediates, datasetTypes.outputs):
            if registerDatasetTypes:
                _LOG.debug("Registering DatasetType %s with registry", datasetType)
                # this is a no-op if it already exists and is consistent,
                # and it raises if it is inconsistent.
                self.butler.registry.registerDatasetType(datasetType)
            else:
                _LOG.debug("Checking DatasetType %s against registry", datasetType)
                expected = self.butler.registry.getDatasetType(datasetType.name)
                if expected != datasetType:
                    raise ValueError(f"DatasetType configuration does not match Registry: "
                                     f"{datasetType} != {expected}")

    def saveInitOutputs(self, graph):
        """Write any datasets produced by initializing tasks in a graph.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.

        Raises
        ------
        Exception
            Raised if ``skipExisting`` is `False` and datasets already
            exists. Content of a butler collection may be changed if
            exception is raised.

        Note
        ----
        If ``skipExisting`` is `True` then existing datasets are not
        overwritten, instead we should check that their stored object is
        exactly the same as what we would save at this time. Comparing
        arbitrary types of object is, of course, non-trivial. Current
        implementation only checks the existence of the datasets and their
        types against the types of objects produced by tasks. Ideally we
        would like to check that object data is identical too but presently
        there is no generic way to compare objects. In the future we can
        potentially introduce some extensible mechanism for that.
        """
        _LOG.debug("Will save InitOutputs for all tasks")
        for taskNodes in graph:
            taskDef = taskNodes.taskDef
            task = self.taskFactory.makeTask(taskDef.taskClass, taskDef.config, None, self.butler)
            for name in taskDef.connections.initOutputs:
                attribute = getattr(taskDef.connections, name)
                initOutputVar = getattr(task, name)
                objFromStore = None
                if self.skipExisting:
                    # check if it is there already
                    _LOG.debug("Retrieving InitOutputs for task=%s key=%s dsTypeName=%s",
                               task, name, attribute.name)
                    objFromStore = self.butler.get(attribute.name, {})
                    if objFromStore is not None:
                        # Types are supposed to be identical.
                        # TODO: Check that object contents is identical too.
                        if type(objFromStore) is not type(initOutputVar):
                            raise TypeError(f"Stored initOutput object type {type(objFromStore)} "
                                            f"is different  from task-generated type "
                                            f"{type(initOutputVar)} for task {taskDef}")
                if objFromStore is None:
                    # butler will raise exception if dataset is already there
                    _LOG.debug("Saving InitOutputs for task=%s key=%s", task, name)
                    self.butler.put(initOutputVar, attribute.name, {})
