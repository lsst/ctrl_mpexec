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

import os
import shutil
import tempfile
import unittest

import lsst.daf.butler
import lsst.utils.tests
from lsst.ctrl.mpexec import SimplePipelineExecutor
from lsst.pipe.base import TaskDef
from lsst.pipe.base.tests.no_dimensions import NoDimensionsTestTask


class SimplePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SimplePipelineExecutor API with a trivial task."""

    def setUp(self):
        self.path = tempfile.mkdtemp()
        lsst.daf.butler.Butler.makeRepo(self.path)
        self.butler = SimplePipelineExecutor.prep_butler(self.path, [], "fake")
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "input",
                dimensions=self.butler.registry.dimensions.empty,
                storageClass="StructuredDataDict",
            )
        )
        self.butler.put({"zero": 0}, "input")

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_from_task_class(self):
        """Test executing a single quantum with an executor created by the
        `from_task_class` factory method, and the
        `SimplePipelineExecutor.as_generator` method.
        """
        executor = SimplePipelineExecutor.from_task_class(NoDimensionsTestTask, butler=self.butler)
        (quantum,) = executor.as_generator(register_dataset_types=True)
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1})

    def test_from_pipeline(self):
        """Test executing a two quanta from different configurations of the
        same task, with an executor created by the `from_pipeline` factory
        method, and the `SimplePipelineExecutor.run` method.
        """
        config_a = NoDimensionsTestTask.ConfigClass()
        config_a.connections.output = "intermediate"
        config_b = NoDimensionsTestTask.ConfigClass()
        config_b.connections.input = "intermediate"
        config_b.key = "two"
        config_b.value = 2
        task_defs = [
            TaskDef(label="a", taskClass=NoDimensionsTestTask, config=config_a),
            TaskDef(label="b", taskClass=NoDimensionsTestTask, config=config_b),
        ]
        executor = SimplePipelineExecutor.from_pipeline(task_defs, butler=self.butler)
        quanta = executor.run(register_dataset_types=True)
        self.assertEqual(len(quanta), 2)
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_from_pipeline_file(self):
        """Test executing a two quanta from different configurations of the
        same task, with an executor created by the `from_pipeline_filename`
        factory method, and the `SimplePipelineExecutor.run` method.
        """
        filename = os.path.join(self.path, "pipeline.yaml")
        with open(filename, "w") as f:
            f.write(
                """
                description: test
                tasks:
                    a:
                        class: "lsst.pipe.base.tests.no_dimensions.NoDimensionsTestTask"
                        config:
                            connections.output: "intermediate"
                    b:
                        class: "lsst.pipe.base.tests.no_dimensions.NoDimensionsTestTask"
                        config:
                            connections.input: "intermediate"
                            key: "two"
                            value: 2
                """
            )
        executor = SimplePipelineExecutor.from_pipeline_filename(filename, butler=self.butler)
        quanta = executor.run(register_dataset_types=True)
        self.assertEqual(len(quanta), 2)
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
