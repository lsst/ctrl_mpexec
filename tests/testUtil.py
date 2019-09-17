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

"""Bunch of common classes and methods for use in unit tests.
"""

__all__ = ["AddTaskConfig", "AddTask", "AddTaskFactoryMock", "ButlerMock"]

import itertools
import logging
import numpy
import functools
import os
from collections import defaultdict
from types import SimpleNamespace

from lsst.daf.butler import (ButlerConfig, DatasetRef, DimensionUniverse,
                             DatasetType, Registry, Run)
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.pipe.base import connectionTypes as cT

_LOG = logging.getLogger(__name__)


class AddTaskConnections(pipeBase.PipelineTaskConnections,
                         dimensions=("instrument", "detector")):
    input = cT.Input(name="add_input",
                     dimensions=["instrument", "detector"],
                     storageClass="NumpyArray",
                     doc="Input dataset type for this task")
    output = cT.Output(name="add_output",
                       dimensions=["instrument", "detector"],
                       storageClass="NumpyArray",
                       doc="Output dataset type for this task")
    initout = cT.InitOutput(name="add_init_output",
                            storageClass="NumpyArray",
                            doc="Init Output dataset type for this task")


class AddTaskConfig(pipeBase.PipelineTaskConfig,
                    pipelineConnections=AddTaskConnections):
    addend = pexConfig.Field(doc="amount to add", dtype=int, default=3)


# example task which overrides run() method
class AddTask(pipeBase.PipelineTask):
    ConfigClass = AddTaskConfig
    _DefaultName = "add_task"

    initout = numpy.array([999])
    """InitOutputs for this task"""

    countExec = 0
    """Number of times run() method was called for this class"""

    stopAt = -1
    """Raises exception at this call to run()"""

    def run(self, input):
        if AddTask.stopAt == AddTask.countExec:
            raise RuntimeError("pretend something bad happened")
        AddTask.countExec += 1
        self.metadata.add("add", self.config.addend)
        output = [val + self.config.addend for val in input]
        _LOG.info("input = %s, output = %s", input, output)
        return pipeBase.Struct(output=output)


class AddTaskFactoryMock(pipeBase.TaskFactory):
    def loadTaskClass(self, taskName):
        if taskName == "AddTask":
            return AddTask, "AddTask"

    def makeTask(self, taskClass, config, overrides, butler):
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        return taskClass(config=config, initInputs=None)


class ButlerMock:
    """Mock version of butler, only usable for testing

    Parameters
    ----------
    fullRegistry : `boolean`, optional
        If True then instantiate SQLite registry with default configuration.
        If False then registry is just a namespace with `dimensions` attribute
        containing DimensionUniverse from default configuration.
    """
    def __init__(self, fullRegistry=False, collection="TestColl"):
        self.datasets = {}
        self.fullRegistry = fullRegistry
        if self.fullRegistry:
            testDir = os.path.dirname(__file__)
            configFile = os.path.join(testDir, "config/butler.yaml")
            butlerConfig = ButlerConfig(configFile)
            self.registry = Registry.fromConfig(butlerConfig, create=True)
            self.run = self.registry.makeRun(collection)
        else:
            self.registry = SimpleNamespace(dimensions=DimensionUniverse.fromConfig())
            self.run = Run(collection=collection, environment=None, pipeline=None)

    def _standardizeArgs(self, datasetRefOrType, dataId=None, **kwds):
        """Copied from real Butler
        """
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None or kwds:
                raise ValueError("DatasetRef given, cannot use dataId as well")
            datasetType = datasetRefOrType.datasetType
            dataId = datasetRefOrType.dataId
        else:
            # Don't check whether DataId is provided, because Registry APIs
            # can usually construct a better error message when it wasn't.
            if isinstance(datasetRefOrType, DatasetType):
                datasetType = datasetRefOrType
            else:
                datasetType = self.registry.getDatasetType(datasetRefOrType)
        return datasetType, dataId

    @staticmethod
    def key(dataId):
        """Make a dict key out of dataId.
        """
        return frozenset(dataId.items())

    def get(self, datasetRefOrType, dataId=None, parameters=None, **kwds):
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        _LOG.info("butler.get: datasetType=%s dataId=%s", datasetType.name, dataId)
        dsTypeName = datasetType.name
        key = self.key(dataId)
        dsdata = self.datasets.get(dsTypeName)
        if dsdata:
            return dsdata.get(key)
        return None

    def put(self, obj, datasetRefOrType, dataId=None, producer=None, **kwds):
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        _LOG.info("butler.put: datasetType=%s dataId=%s obj=%r", datasetType.name, dataId, obj)
        dsTypeName = datasetType.name
        key = self.key(dataId)
        dsdata = self.datasets.setdefault(dsTypeName, {})
        dsdata[key] = obj
        if self.fullRegistry:
            ref = self.registry.addDataset(datasetType, dataId, run=self.run, producer=producer,
                                           recursive=False, **kwds)
        else:
            # we should return DatasetRef with reasonable ID, ID is supposed to be unique
            refId = sum(len(val) for val in self.datasets.values())
            ref = DatasetRef(datasetType, dataId, id=refId)
        return ref

    def remove(self, datasetRefOrType, dataId=None, *, delete=True, remember=True, **kwds):
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        _LOG.info("butler.remove: datasetType=%s dataId=%s", datasetType.name, dataId)
        dsTypeName = datasetType.name
        key = self.key(dataId)
        dsdata = self.datasets.get(dsTypeName)
        del dsdata[key]
        ref = self.registry.find(self.run.collection, datasetType, dataId, **kwds)
        if remember:
            self.registry.disassociate(self.run.collection, [ref])
        else:
            self.registry.removeDataset(ref)


def registerDatasetTypes(registry, pipeline):
    """Register all dataset types used by tasks in a registry.

    Copied and modified from `PreExecInit.initializeDatasetTypes`.

    Parameters
    ----------
    registry : `~lsst.daf.butler.Registry`
        Registry instance.
    pipeline : `~lsst.pipe.base.Pipeline`
        Iterable of TaskDef instances.
    """
    for taskDef in pipeline:
        datasetTypes = pipeBase.TaskDatasetTypes.fromConnections(taskDef.connections,
                                                                 registry=registry)
        for datasetType in itertools.chain(datasetTypes.initInputs, datasetTypes.initOutputs,
                                           datasetTypes.inputs, datasetTypes.outputs,
                                           datasetTypes.prerequisites):
            _LOG.info("Registering %s with registry", datasetType)
            # this is a no-op if it already exists and is consistent,
            # and it raises if it is inconsistent.
            registry.registerDatasetType(datasetType)


def makeSimpleQGraph(nQuanta=5, pipeline=None, butler=None, skipExisting=False, clobberExisting=False):
    """Make simple QuantumGraph for tests.

    Makes simple one-task pipeline with AddTask, sets up in-memory
    registry and butler, fills them with minimal data, and generates
    QuantumGraph with all of that.

    Parameters
    ----------
    nQuanta : `int`
        Number of quanta in a graph.
    pipeline : `~lsst.pipe.base.Pipeline`
        If `None` then one-task pipeline is made with `AddTask` and
        default `AddTaskConfig`.
    butler : `~lsst.daf.butler.Butler`, optional
        Data butler instance, this should be an instance returned from a
        previous call to this method.
    skipExisting : `bool`, optional
        If `True` (default), a Quantum is not created if all its outputs
        already exist.
    clobberExisting : `bool`, optional
        If `True`, overwrite any outputs that already exist.  Cannot be
        `True` if ``skipExisting`` is.

    Returns
    -------
    butler : `~lsst.daf.butler.Butler`
        Butler instance
    qgraph : `~lsst.pipe.base.QuantumGraph`
        Quantum graph instance
    """

    if pipeline is None:
        taskDef = pipeBase.TaskDef("AddTask", AddTaskConfig(), taskClass=AddTask, label="task1")
        pipeline = pipeBase.Pipeline([taskDef])

    if butler is None:

        butler = ButlerMock(fullRegistry=True)

        # Add dataset types to registry
        registerDatasetTypes(butler.registry, pipeline)

        # Small set of DataIds included in QGraph
        records = [dict(instrument="INSTR", id=i, full_name=str(i)) for i in range(nQuanta)]
        dataIds = [dict(instrument="INSTR", detector=detector) for detector in range(nQuanta)]

        # Add all needed dimensions to registry
        butler.registry.insertDimensionData("instrument", dict(name="INSTR"))
        butler.registry.insertDimensionData("detector", *records)

        # Add inputs to butler
        for i, dataId in enumerate(dataIds):
            data = numpy.array([i, 10*i])
            butler.put(data, "add_input", dataId)

    # Make the graph, task factory is not needed here
    builder = pipeBase.GraphBuilder(taskFactory=None, registry=butler.registry,
                                    skipExisting=skipExisting, clobberExisting=clobberExisting)
    qgraph = builder.makeGraph(
        pipeline,
        inputCollections=defaultdict(functools.partial(list, [butler.run.collection])),
        outputCollection=butler.run.collection,
        userQuery=""
    )

    return butler, qgraph
