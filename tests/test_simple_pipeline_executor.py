# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import os
import shutil
import tempfile
import unittest

import lsst.daf.butler
import lsst.utils.tests
from lsst.ctrl.mpexec import SimplePipelineExecutor
from lsst.pipe.base import PipelineGraph
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    MockStorageClass,
    get_mock_name,
)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SimplePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SimplePipelineExecutor API.

    Because SimplePipelineExecutor is the easiest way to run simple pipelines
    in tests, this has also become a home for tests of execution edge cases
    that don't have a clear home in other test files.
    """

    def setUp(self):
        self.path = tempfile.mkdtemp()
        # standalone parameter forces the returned config to also include
        # the information from the search paths.
        config = lsst.daf.butler.Butler.makeRepo(
            self.path, standalone=True, searchPaths=[os.path.join(TESTDIR, "config")]
        )
        self.butler = SimplePipelineExecutor.prep_butler(config, [], "fake")
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "input",
                dimensions=self.butler.dimensions.empty,
                storageClass="StructuredDataDict",
            )
        )
        self.butler.put({"zero": 0}, "input")
        MockStorageClass.get_or_register_mock("StructuredDataDict")
        MockStorageClass.get_or_register_mock("TaskMetadataLike")

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_from_task_class(self):
        """Test executing a single quantum with an executor created by the
        `from_task_class` factory method, and the
        `SimplePipelineExecutor.as_generator` method.
        """
        config = DynamicTestPipelineTaskConfig()
        config.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        executor = SimplePipelineExecutor.from_task_class(
            DynamicTestPipelineTask,
            config=config,
            butler=self.butler,
            label="a",
        )
        (quantum,) = executor.as_generator(register_dataset_types=True)
        self.assertEqual(self.butler.get("output").storage_class, get_mock_name("StructuredDataDict"))

    def test_metadata_input(self):
        """Test two tasks where the output uses metadata from input."""
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_b = DynamicTestPipelineTaskConfig()
        config_b.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        config_b.inputs["in_metadata"] = DynamicConnectionConfig(
            dataset_type_name="a_metadata", storage_class="TaskMetadata"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, config_b)
        executor = SimplePipelineExecutor.from_pipeline_graph(pipeline_graph, butler=self.butler)
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        output = self.butler.get("output")
        self.assertEqual(output.quantum.inputs["in_metadata"][0].original_type, "lsst.pipe.base.TaskMetadata")

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
                        class: "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask"
                        config:
                            python: |
                                from lsst.pipe.base.tests.mocks import DynamicConnectionConfig
                                config.inputs["i"] = DynamicConnectionConfig(
                                    dataset_type_name="input",
                                    storage_class="StructuredDataDict",
                                    mock_storage_class=False,
                                )
                                config.outputs["o"] = DynamicConnectionConfig(
                                    dataset_type_name="intermediate",
                                    storage_class="StructuredDataDict",
                                )
                    b:
                        class: "lsst.pipe.base.tests.mocks.DynamicTestPipelineTask"
                        config:
                            python: |
                                from lsst.pipe.base.tests.mocks import DynamicConnectionConfig
                                config.inputs["i"] = DynamicConnectionConfig(
                                    dataset_type_name="intermediate",
                                    storage_class="StructuredDataDict",
                                )
                                config.outputs["o"] = DynamicConnectionConfig(
                                    dataset_type_name="output",
                                    storage_class="StructuredDataDict",
                                )
                """
            )
        executor = SimplePipelineExecutor.from_pipeline_filename(filename, butler=self.butler)
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self.assertEqual(self.butler.get("intermediate").storage_class, get_mock_name("StructuredDataDict"))
        self.assertEqual(self.butler.get("output").storage_class, get_mock_name("StructuredDataDict"))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    """Generic tests for file leaks."""


def setup_module(module):
    """Set up the module for pytest.

    Parameters
    ----------
    module : `~types.ModuleType`
        Module to set up.
    """
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
