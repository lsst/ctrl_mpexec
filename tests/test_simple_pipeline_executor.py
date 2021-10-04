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

import shutil
import tempfile
import unittest

import lsst.daf.butler
from lsst.ctrl.mpexec import SimplePipelineExecutor
from lsst.pipe.base import PipelineTask, PipelineTaskConfig, PipelineTaskConnections, connectionTypes, Struct

import lsst.utils.tests


class FakeConnections(PipelineTaskConnections, dimensions=set()):
    input = connectionTypes.Input(
        name="fakeInput", doc="some dict-y input data for testing", storageClass="StructuredDataDict"
    )
    output = connectionTypes.Output(
        name="fakeOutput", doc="some dict-y input data for testing", storageClass="StructuredDataDict"
    )


class FakeConfig(PipelineTaskConfig, pipelineConnections=FakeConnections):
    pass


class FakeTask(PipelineTask):
    ConfigClass = FakeConfig
    _DefaultName = "fakeTask"

    def run(self, input):
        result = {1: "one", 2: "two"}
        result.update(input)
        return Struct(output=result, other=["a", "b", "c"])


class SimplePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SimplePipelineExecutor API with a trivial task."""

    def setUp(self):
        self.path = tempfile.mkdtemp()
        lsst.daf.butler.Butler.makeRepo(self.path)
        self.butler = SimplePipelineExecutor.prep_butler(self.path, [], "fake", writeable=True)

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_from_task_class(self):
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "fakeInput",
                dimensions=self.butler.registry.dimensions.empty,
                storageClass="StructuredDataDict",
            )
        )
        self.butler.put({3: "three", 4: "four"}, "fakeInput")
        executor = SimplePipelineExecutor.from_task_class(FakeTask, butler=self.butler)
        quanta = executor.run(register_dataset_types=True)
        self.assertEqual(len(quanta), 1)
        self.assertEqual(self.butler.get("fakeOutput"), {1: "one", 2: "two", 3: "three", 4: "four"})

    def test_generator_from_task_class(self):
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "fakeInput",
                dimensions=self.butler.registry.dimensions.empty,
                storageClass="StructuredDataDict",
            )
        )
        self.butler.put({3: "three", 4: "four"}, "fakeInput")
        executor = SimplePipelineExecutor.from_task_class(FakeTask, butler=self.butler)
        quanta = list(executor.as_generator(register_dataset_types=True))
        self.assertEqual(len(quanta), 1)
        self.assertEqual(self.butler.get("fakeOutput"), {1: "one", 2: "two", 3: "three", 4: "four"})


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
