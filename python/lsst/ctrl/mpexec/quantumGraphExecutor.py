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

__all__ = ['QuantumGraphExecutor']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from abc import ABC, abstractmethod
import logging

# -----------------------------
#  Imports for other modules --
# -----------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


class QuantumGraphExecutor(ABC):
    """Class with partial implementation of QuantumGraph execution.

    Any specific execution model is implemented in sub-class by overriding
    the `executeQuanta` method.
    """

    def execute(self, graph, butler, taskFactory, registerDatasetTypes=False,
                saveInitOutputs=True, updateOutputCollection=True, initOnly=False):
        """Execute whole graph.

        Parameters
        ----------
        graph : `~lsst.pip.base.QuantumGraph`
            Execution graph.
        butler : `~lsst.daf.butler.Butler`
            Data butler instance
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.
        saveInitOutputs : `bool`, optional
            If ``True`` (default) then save task "init outputs" to butler.
        updateOutputCollection : `bool`, optional
            If ``True`` (default) then copy all inputs to output collection.
        initOnly : `bool`, optional
            if ``True`` then only initialization part is run, but not any
            task.
        """
        # register dataset types or check consistency
        for datasetType in graph.getDatasetTypes():
            if registerDatasetTypes:
                # this is a no-op if it already exists and is consistent,
                # and it raises if it is inconsistent.
                butler.registry.registerDatasetType(datasetType)
            else:
                expected = butler.registry.getDatasetType(datasetType.name)
                if expected != datasetType:
                    raise ValueError(f"DatasetType configuration does not match Registry: "
                                     f"{datasetType} != {expected}")

        # associate all existing datasets with output collection.
        if updateOutputCollection:
            self.updateOutputCollection(graph, butler)

        # Save task initialization data.
        # TODO: see if Pipeline and software versions are already written
        # to butler and associated with Run, check for consistency if they
        # are, and if so skip writing TaskInitOutputs (because those should
        # also only be done once).  If not, write them.
        if saveInitOutputs:
            _LOG.debug("Will save InitOutputs for all tasks")
            for taskNodes in graph:
                taskDef = taskNodes.taskDef
                task = taskFactory.makeTask(taskDef.taskClass, taskDef.config, None, butler)
                self.writeTaskInitOutputs(task, butler)

        # May want to stop here
        if initOnly:
            return

        # call subclass-provided method to execute individual Quanta
        self.executeQuanta(graph.traverse(), butler, taskFactory)

    def updateOutputCollection(self, graph, butler):
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
        butler : `~lsst.daf.butler.Butler`
            data butler instance
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
                   len(id2ref), butler.run.collection)
        if id2ref:
            # copy all collected refs to output collection
            collection = butler.run.collection
            registry = butler.registry
            registry.associate(collection, list(id2ref.values()))

    def writeTaskInitOutputs(self, task, butler):
        """Write any datasets produced by initializing the given PipelineTask.

        Parameters
        ----------
        task : `~lsst.pipe.base.PipelineTask`
            Instance of PipelineTask
        butler : `~lsst.daf.butler.Butler`
            Data butler instance
        """
        initOutputs = task.getInitOutputDatasets()
        initOutputDatasetTypes = task.getInitOutputDatasetTypes(task.config)
        for key, obj in initOutputs.items():
            _LOG.debug("Saving InitOutputs for task=%s key=%s", task, key)
            butler.put(obj, initOutputDatasetTypes[key].datasetType, {})

    @abstractmethod
    def executeQuanta(self, iterable, butler, taskFactory):
        """Execute all individual Quanta.

        Implementation of this method depends on particular execution model
        and it has to be provided by a subclass. Execution model determines
        what happens here; it can be either actual running of the task or,
        for example, generation of the scripts for delayed batch execution.

        Any exception raised in this method will be propagated to the caller
        of `execute` method.

        Parameters
        ----------
        iterable : iterable of `~lsst.pipe.base.QuantumIterData`
            Sequence of Quanta to execute. It is guaranteed that pre-requisites
            for a given Quantum will always appear before that Quantum.
        butler : `~lsst.daf.butler.Butler`
            Data butler instance
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        """
