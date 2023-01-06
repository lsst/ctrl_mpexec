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
from typing import Any, Dict

import lsst.daf.butler
import lsst.utils.tests
from lsst.ctrl.mpexec import SimplePipelineExecutor
from lsst.pex.config import Field
from lsst.pipe.base import (
    PipelineTask,
    PipelineTaskConfig,
    PipelineTaskConnections,
    Struct,
    TaskDef,
    TaskMetadata,
    connectionTypes,
)
from lsst.pipe.base.tests.no_dimensions import NoDimensionsTestTask
from lsst.utils.introspection import get_full_type_name

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class NoDimensionsTestConnections2(PipelineTaskConnections, dimensions=set()):
    input = connectionTypes.Input(
        name="input", doc="some dict-y input data for testing", storageClass="TaskMetadataLike"
    )
    output = connectionTypes.Output(
        name="output", doc="some dict-y output data for testing", storageClass="StructuredDataDict"
    )


class NoDimensionsTestConfig2(PipelineTaskConfig, pipelineConnections=NoDimensionsTestConnections2):
    key = Field(dtype=str, doc="String key for the dict entry the task sets.", default="one")
    value = Field(dtype=int, doc="Integer value for the dict entry the task sets.", default=1)
    outputSC = Field(dtype=str, doc="Output storage class requested", default="dict")


class NoDimensionsMetadataTestConnections(PipelineTaskConnections, dimensions=set()):
    input = connectionTypes.Input(
        name="input", doc="some dict-y input data for testing", storageClass="StructuredDataDict"
    )
    # Deliberately choose a storage class that does not match the metadata
    # default TaskMetadata storage class.
    meta = connectionTypes.Input(
        name="a_metadata", doc="Metadata from previous task", storageClass="StructuredDataDict"
    )
    output = connectionTypes.Output(
        name="output", doc="some dict-y output data for testing", storageClass="StructuredDataDict"
    )


class NoDimensionsMetadataTestConfig(
    PipelineTaskConfig, pipelineConnections=NoDimensionsMetadataTestConnections
):
    key = Field(dtype=str, doc="String key for the dict entry the task sets.", default="one")
    value = Field(dtype=int, doc="Integer value for the dict entry the task sets.", default=1)
    outputSC = Field(dtype=str, doc="Output storage class requested", default="dict")


class NoDimensionsMetadataTestTask(PipelineTask):
    """A simple pipeline task that can take a metadata as input."""

    ConfigClass = NoDimensionsMetadataTestConfig
    _DefaultName = "noDimensionsMetadataTest"

    def run(self, input: Dict[str, int], meta: Dict[str, Any]) -> Struct:
        """Run the task, adding the configured key-value pair to the input
        argument and returning it as the output.

        Parameters
        ----------
        input : `dict`
            Dictionary to update and return.

        Returns
        -------
        result : `lsst.pipe.base.Struct`
            Struct with a single ``output`` attribute.
        """
        self.log.info("Run metadata method given data of type: %s", get_full_type_name(input))
        output = input.copy()
        output[self.config.key] = self.config.value

        self.log.info("Received task metadata (%s): %s", get_full_type_name(meta), meta)

        # Can change the return type via configuration.
        if "TaskMetadata" in self.config.outputSC:
            output = TaskMetadata.from_dict(output)
        elif type(output) == TaskMetadata:
            # Want the output to be a dict
            output = output.to_dict()
        self.log.info("Run method returns data of type: %s", get_full_type_name(output))
        return Struct(output=output)


class SimplePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SimplePipelineExecutor API with a trivial task."""

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

    def _configure_pipeline(self, config_a_cls, config_b_cls, storageClass_a=None, storageClass_b=None):
        """Configure a pipeline with from_pipeline."""

        config_a = config_a_cls()
        config_a.connections.output = "intermediate"
        if storageClass_a:
            config_a.outputSC = storageClass_a
        config_b = config_b_cls()
        config_b.connections.input = "intermediate"
        if storageClass_b:
            config_b.outputSC = storageClass_b
        config_b.key = "two"
        config_b.value = 2
        task_defs = [
            TaskDef(label="a", taskClass=NoDimensionsTestTask, config=config_a),
            TaskDef(label="b", taskClass=NoDimensionsTestTask, config=config_b),
        ]
        executor = SimplePipelineExecutor.from_pipeline(task_defs, butler=self.butler)
        return executor

    def _test_logs(self, log_output, input_type_a, output_type_a, input_type_b, output_type_b):
        """Check the expected input types received by tasks A and B"""
        all_logs = "\n".join(log_output)
        self.assertIn(f"lsst.a:Run method given data of type: {input_type_a}", all_logs)
        self.assertIn(f"lsst.b:Run method given data of type: {input_type_b}", all_logs)
        self.assertIn(f"lsst.a:Run method returns data of type: {output_type_a}", all_logs)
        self.assertIn(f"lsst.b:Run method returns data of type: {output_type_b}", all_logs)

    def test_from_pipeline(self):
        """Test executing a two quanta from different configurations of the
        same task, with an executor created by the `from_pipeline` factory
        method, and the `SimplePipelineExecutor.run` method.
        """
        executor = self._configure_pipeline(
            NoDimensionsTestTask.ConfigClass, NoDimensionsTestTask.ConfigClass
        )

        with self.assertLogs("lsst", level="INFO") as cm:
            quanta = executor.run(register_dataset_types=True, save_versions=False)
        self._test_logs(cm.output, "dict", "dict", "dict", "dict")

        self.assertEqual(len(quanta), 2)
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_from_pipeline_intermediates_differ(self):
        """Run pipeline but intermediates definition in registry differs."""
        executor = self._configure_pipeline(
            NoDimensionsTestTask.ConfigClass,
            NoDimensionsTestTask.ConfigClass,
            storageClass_b="TaskMetadataLike",
        )

        # Pre-define the "intermediate" storage class to be something that is
        # like a dict but is not a dict. This will fail unless storage
        # class conversion is supported in put and get.
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "intermediate",
                dimensions=self.butler.registry.dimensions.empty,
                storageClass="TaskMetadataLike",
            )
        )

        with self.assertLogs("lsst", level="INFO") as cm:
            quanta = executor.run(register_dataset_types=True, save_versions=False)
        # A dict is given to task a without change.
        # A returns a dict because it has not been told to do anything else.
        # That does not match the storage class so it will be converted
        # on put.
        # b is given a dict, because that's what its connection asks for.
        # b returns a TaskMetadata because that's how we configured it, but
        # the butler expects a dict so it is converted on put.
        self._test_logs(cm.output, "dict", "dict", "dict", "lsst.pipe.base.TaskMetadata")

        self.assertEqual(len(quanta), 2)
        self.assertEqual(self.butler.get("intermediate").to_dict(), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_from_pipeline_output_differ(self):
        """Run pipeline but output definition in registry differs."""
        executor = self._configure_pipeline(
            NoDimensionsTestTask.ConfigClass,
            NoDimensionsTestTask.ConfigClass,
            storageClass_a="TaskMetadataLike",
        )

        # Pre-define the "output" storage class to be something that is
        # like a dict but is not a dict. This will fail unless storage
        # class conversion is supported in put and get.
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "output",
                dimensions=self.butler.registry.dimensions.empty,
                storageClass="TaskMetadataLike",
            )
        )

        with self.assertLogs("lsst", level="INFO") as cm:
            quanta = executor.run(register_dataset_types=True, save_versions=False)
        # a has been told to return a TaskMetadata but will convert to dict.
        # b returns a dict and that is converted to TaskMetadata on put.
        self._test_logs(cm.output, "dict", "lsst.pipe.base.TaskMetadata", "dict", "dict")

        self.assertEqual(len(quanta), 2)
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output").to_dict(), {"zero": 0, "one": 1, "two": 2})

    def test_from_pipeline_input_differ(self):
        """Run pipeline but input definition in registry differs."""

        # This config declares that the pipeline takes a TaskMetadata
        # as input but registry already thinks it has a StructureDataDict.
        executor = self._configure_pipeline(NoDimensionsTestConfig2, NoDimensionsTestTask.ConfigClass)

        with self.assertLogs("lsst", level="INFO") as cm:
            quanta = executor.run(register_dataset_types=True, save_versions=False)
        self._test_logs(cm.output, "lsst.pipe.base.TaskMetadata", "dict", "dict", "dict")

        self.assertEqual(len(quanta), 2)
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_from_pipeline_incompatible(self):
        """Run pipeline but definitions are not compatible."""
        executor = self._configure_pipeline(
            NoDimensionsTestTask.ConfigClass, NoDimensionsTestTask.ConfigClass
        )

        # Incompatible output dataset type.
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "output",
                dimensions=self.butler.registry.dimensions.empty,
                storageClass="StructuredDataList",
            )
        )

        with self.assertRaisesRegex(
            ValueError, "StructuredDataDict.*inconsistent with registry definition.*StructuredDataList"
        ):
            executor.run(register_dataset_types=True, save_versions=False)

    def test_from_pipeline_metadata(self):
        """Test two tasks where the output uses metadata from input."""
        # Must configure a special pipeline for this test.
        config_a = NoDimensionsTestTask.ConfigClass()
        config_a.connections.output = "intermediate"
        config_b = NoDimensionsMetadataTestTask.ConfigClass()
        config_b.connections.input = "intermediate"
        config_b.key = "two"
        config_b.value = 2
        task_defs = [
            TaskDef(label="a", taskClass=NoDimensionsTestTask, config=config_a),
            TaskDef(label="b", taskClass=NoDimensionsMetadataTestTask, config=config_b),
        ]
        executor = SimplePipelineExecutor.from_pipeline(task_defs, butler=self.butler)

        with self.assertLogs("test_simple_pipeline_executor", level="INFO") as cm:
            quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertIn(f"Received task metadata ({get_full_type_name(dict)})", "".join(cm.output))

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
        quanta = executor.run(register_dataset_types=True, save_versions=False)
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
