# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Simple unit test for Pipeline.
"""

from collections import namedtuple
import io
import unittest

from lsst.daf.butler import DimensionNameSet
from lsst.pipe.base import (PipelineTask, PipelineTaskConfig, multiplicity,
                            InputDatasetField, OutputDatasetField,
                            DatasetTypeDescriptor, Pipeline, TaskDef)
from lsst.ctrl.mpexec.dotTools import pipeline2dot
import lsst.utils.tests

# mock for actual dataset type
DS = namedtuple("DS", "name dimensions")


# This method is used by PipelineTask to instanciate DatasetType, normally this
# should come from some other module but we have not defined that yet, so I
# stick a trivial (mock) implementation here.
def makeDatasetTypeDescr(dsConfig):
    datasetType = DS(name=dsConfig.name, dimensions=DimensionNameSet(dsConfig.dimensions))
    return DatasetTypeDescriptor(datasetType, manualLoad=False)


class ExamplePipelineTaskConfig(PipelineTaskConfig):
    input1 = InputDatasetField(name="",
                               dimensions=[],
                               storageClass="example",
                               doc="Input for this task")
    input2 = InputDatasetField(name="",
                               dimensions=[],
                               storageClass="example",
                               doc="Input for this task")
    output1 = OutputDatasetField(name="",
                                 dimensions=[],
                                 storageClass="example",
                                 doc="Output for this task")
    output2 = OutputDatasetField(name="",
                                 dimensions=[],
                                 storageClass="example",
                                 doc="Output for this task")


def _makeConfig(inputName, outputName):
    """Factory method for config instances

    inputName and outputName can be either string or tuple of strings
    with two items max.
    """
    config = ExamplePipelineTaskConfig()
    if isinstance(inputName, tuple):
        config.input1.name = inputName[0]
        config.input2.name = inputName[1] if len(inputName) > 1 else ""
    else:
        config.input1.name = inputName

    if isinstance(outputName, tuple):
        config.output1.name = outputName[0]
        config.output2.name = outputName[1] if len(outputName) > 1 else ""
    else:
        config.output1.name = outputName

    dimensions = ["Visit", "Detector"]
    config.input1.dimensions = dimensions
    config.input2.dimensions = dimensions
    config.output1.dimensions = dimensions
    config.output2.dimensions = dimensions

    return config


class ExamplePipelineTask(PipelineTask):
    ConfigClass = ExamplePipelineTaskConfig

    @classmethod
    def getInputDatasetTypes(cls, config):
        types = {"input1": makeDatasetTypeDescr(config.input1)}
        if config.input2.name:
            types["input2"] = makeDatasetTypeDescr(config.input2)
        return types

    @classmethod
    def getOutputDatasetTypes(cls, config):
        types = {"output1": makeDatasetTypeDescr(config.output1)}
        if config.output2.name:
            types["output2"] = makeDatasetTypeDescr(config.output2)
        return types

    @classmethod
    def getDatasetTypeMultiplicities(cls, config):
        result = dict(input1=multiplicity.Multiple(),
                      output2=multiplicity.Mulitple())
        if config.input2.name:
            result.update(input2=multiplicity.Multiple())
        if config.output2.name:
            result.update(output2=multiplicity.Multiple())


def _makePipeline(tasks):
    """Generate Pipeline instance.

    Parameters
    ----------
    tasks : list of tuples
        Each tuple in the list has 3 or 4 items:
        - input DatasetType name(s), string or tuple of strings
        - output DatasetType name(s), string or tuple of strings
        - task label, string
        - optional task class object, can be None

    Returns
    -------
    Pipeline instance
    """
    pipe = Pipeline()
    for task in tasks:
        inputs = task[0]
        outputs = task[1]
        label = task[2]
        klass = task[3] if len(task) > 3 else ExamplePipelineTask
        config = _makeConfig(inputs, outputs)
        pipe.append(TaskDef("ExamplePipelineTask", config, klass, label))
    return pipe


class DotToolsTestCase(unittest.TestCase):
    """A test case for dotTools
    """

    def testPipeline2dot(self):
        """Tests for dotTools.pipeline2dot method
        """
        pipeline = _makePipeline([("A", ("B", "C"), "task1"),
                                  ("C", "E", "task2"),
                                  ("B", "D", "task3"),
                                  (("D", "E"), "F", "task4")])
        file = io.StringIO()
        pipeline2dot(pipeline, file)

        # it's hard to validate complete output, just checking few basic things,
        # even that is not terribly stable
        lines = file.getvalue().strip().split('\n')
        ndatasets = 6
        ntasks = 4
        nedges = 10
        nextra = 2  # graph header and closing
        self.assertEqual(len(lines), ndatasets + ntasks + nedges + nextra)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
