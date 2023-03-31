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
import tempfile
import unittest

import lsst.daf.butler
import lsst.daf.butler.tests as butlerTests
import lsst.utils.tests
from lsst.ctrl.mpexec import SeparablePipelineExecutor
from lsst.pipe.base import Instrument, Pipeline, TaskMetadata
from lsst.resources import ResourcePath

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SeparablePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SeparablePipelineExecutor API with a trivial task."""

    pipeline_file = os.path.join(TESTDIR, "pipeline_separable.yaml")

    def setUp(self):
        repodir = tempfile.TemporaryDirectory()
        # TemporaryDirectory warns on leaks; addCleanup also keeps it from
        # getting garbage-collected.
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, repodir)

        # standalone parameter forces the returned config to also include
        # the information from the search paths.
        config = lsst.daf.butler.Butler.makeRepo(
            repodir.name, standalone=True, searchPaths=[os.path.join(TESTDIR, "config")]
        )
        butler = lsst.daf.butler.Butler(config, writeable=True)
        output = "fake"
        output_run = f"{output}/{Instrument.makeCollectionTimestamp()}"
        butler.registry.registerCollection(output_run, lsst.daf.butler.CollectionType.RUN)
        butler.registry.registerCollection(output, lsst.daf.butler.CollectionType.CHAINED)
        butler.registry.setCollectionChain(output, [output_run])
        self.butler = lsst.daf.butler.Butler(butler=butler, collections=[output], run=output_run)

        butlerTests.addDatasetType(self.butler, "input", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "intermediate", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "a_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "a_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")

    def test_init_badinput(self):
        butler = lsst.daf.butler.Butler(butler=self.butler, collections=[], run="foo")

        with self.assertRaises(ValueError):
            SeparablePipelineExecutor(butler)

    def test_init_badoutput(self):
        butler = lsst.daf.butler.Butler(butler=self.butler, collections=["foo"])

        with self.assertRaises(ValueError):
            SeparablePipelineExecutor(butler)

    def test_make_pipeline_full(self):
        executor = SeparablePipelineExecutor(self.butler)
        for uri in [
            self.pipeline_file,
            ResourcePath(self.pipeline_file),
            ResourcePath(self.pipeline_file).geturl(),
        ]:
            pipeline = executor.make_pipeline(uri)
            self.assertEqual(len(pipeline), 2)
            self.assertEqual({t.label for t in pipeline}, {"a", "b"})

    def test_make_pipeline_subset(self):
        executor = SeparablePipelineExecutor(self.butler)
        path = self.pipeline_file + "#a"
        for uri in [
            path,
            ResourcePath(path),
            ResourcePath(path).geturl(),
        ]:
            pipeline = executor.make_pipeline(uri)
            self.assertEqual(len(pipeline), 1)
            self.assertEqual({t.label for t in pipeline}, {"a"})

    def test_make_quantum_graph_nowhere_noskip_noclobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_noskip_noclobber_conflict(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")

        with self.assertRaises(lsst.pipe.base.graphBuilder.OutputExistsError):
            executor.make_quantum_graph(pipeline)

    # TODO: need more complex task and Butler to test
    # make_quantum_graph(where=...)

    def test_make_quantum_graph_nowhere_skipnone_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skiptotal_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 1)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skippartial_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")

        with self.assertRaises(lsst.pipe.base.graphBuilder.OutputExistsError):
            executor.make_quantum_graph(pipeline)

    def test_make_quantum_graph_nowhere_noskip_clobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_noskip_clobber_conflict(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")

        graph = executor.make_quantum_graph(pipeline)
        self.assertFalse(graph.isConnected)  # Both tasks run, but can use old values for b
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a", "b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"a", "b"})

    def test_make_quantum_graph_nowhere_skipnone_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skiptotal_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 1)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skippartial_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")

        graph = executor.make_quantum_graph(pipeline)
        self.assertFalse(graph.isConnected)  # Both tasks run, but can use old values for b
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a", "b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"a", "b"})

    def test_make_quantum_graph_noinput(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        graph = executor.make_quantum_graph(pipeline)
        self.assertEqual(len(graph), 0)

    def test_make_quantum_graph_alloutput_skip(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=[self.butler.run])
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put({"zero": 0}, "output")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "b_log")
        self.butler.put(TaskMetadata(), "b_metadata")

        graph = executor.make_quantum_graph(pipeline)
        self.assertEqual(len(graph), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
