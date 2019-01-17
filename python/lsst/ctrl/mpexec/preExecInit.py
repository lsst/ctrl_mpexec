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

# -----------------------------
#  Imports for other modules --
# -----------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


class PreExecInit:
    """Initialization of registry for QuantumGraph execution.

    This class encapsulates all necessary operations that have to be performed
    on butler and registry to prepare them for QuantumGraph execution.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    """
    def __init__(self, butler):
        self.butler = butler

    def initialize(self, graph, taskFactory, registerDatasetTypes=False,
                   saveInitOutputs=True, updateOutputCollection=True):
        """Perform all initialization steps.

        Convenience method to execute all initialization steps. Instead of
        calling this method and providing all options it is also possible to
        call methods individually.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.
        saveInitOutputs : `bool`, optional
            If ``True`` (default) then save task "init outputs" to butler.
        updateOutputCollection : `bool`, optional
            If ``True`` (default) then copy all inputs to output collection.
        """
        # register dataset types or check consistency
        self.inititalizeDatasetTypes(graph, registerDatasetTypes)

        # associate all existing datasets with output collection.
        if updateOutputCollection:
            self.updateOutputCollection(graph)

        # Save task initialization data.
        # TODO: see if Pipeline and software versions are already written
        # to butler and associated with Run, check for consistency if they
        # are, and if so skip writing TaskInitOutputs (because those should
        # also only be done once).  If not, write them.
        if saveInitOutputs:
            self.saveInitOutputs(graph, taskFactory)

    def inititalizeDatasetTypes(self, graph, registerDatasetTypes=False):
        """Save or check DatasetTypes used by the tasks in a graph.

        Iterates over all DatasetTypes for all tasks in a graph and either
        tries to add them to resgistry or compares them to exising ones.

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
        for datasetType in graph.getDatasetTypes():
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

    def updateOutputCollection(self, graph):
        """Associate all existing datasets with output collection.

        For every Quantum in a graph make sure that its existing inputs are
        added to the Butler's output collection.

        For each quantum there are input and output DataRefs. With the
        current implementation of preflight output refs should not exist but
        input refs may belong to a different collection. We want all refs to
        appear in output collection, so we have to "copy" those refs.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        """
        def _refComponents(refs):
            """Return all dataset components recursively"""
            for ref in refs:
                yield ref
                yield from _refComponents(ref.components.values())

        # Main issue here is that the same DataRef can appear as input for
        # many quanta, to keep them unique we first collect them into one
        # dict indexed by dataset id.
        id2ref = {}
        for taskDef, quantum in graph.quanta():
            for refs in quantum.predictedInputs.values():
                for ref in _refComponents(refs):
                    # skip intermediate datasets produced by other tasks
                    if ref.id is not None:
                        id2ref[ref.id] = ref
        for initInput in graph.initInputs:
            id2ref[initInput.id] = initInput
        _LOG.debug("Associating %d datasets with output collection %s",
                   len(id2ref), self.butler.run.collection)
        if id2ref:
            # copy all collected refs to output collection
            collection = self.butler.run.collection
            registry = self.butler.registry
            registry.associate(collection, list(id2ref.values()))

    def saveInitOutputs(self, graph, taskFactory):
        """Write any datasets produced by initializing tasks in a graph.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        """
        _LOG.debug("Will save InitOutputs for all tasks")
        for taskNodes in graph:
            taskDef = taskNodes.taskDef
            task = taskFactory.makeTask(taskDef.taskClass, taskDef.config, None, self.butler)
            initOutputs = task.getInitOutputDatasets()
            initOutputDatasetTypes = task.getInitOutputDatasetTypes(task.config)
            for key, obj in initOutputs.items():
                _LOG.debug("Saving InitOutputs for task=%s key=%s", task, key)
                self.butler.put(obj, initOutputDatasetTypes[key].datasetType, {})
