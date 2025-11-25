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

import logging
import os
import time
import unittest
import unittest.mock

import click.testing

import lsst.utils.tests
from lsst.ctrl.mpexec import PipelineGraphFactory
from lsst.ctrl.mpexec.cli import opt, script
from lsst.ctrl.mpexec.cli.cmd.commands import PipetaskCommand, coverage_context
from lsst.ctrl.mpexec.cli.utils import collect_pipeline_actions
from lsst.ctrl.mpexec.showInfo import ShowInfo
from lsst.daf.butler import CollectionType, MissingCollectionError
from lsst.daf.butler.cli.utils import LogCliRunner
from lsst.pipe.base.mp_graph_executor import MPGraphExecutorError
from lsst.pipe.base.script import transfer_from_graph
from lsst.pipe.base.tests.mocks import DirectButlerRepo, DynamicTestPipelineTaskConfig


class RunTestCase(unittest.TestCase):
    """Test pipetask run command-line."""

    @staticmethod
    def _make_run_args(*args: str, **kwargs: object) -> dict[str, object]:
        mock = unittest.mock.Mock()

        @click.command(cls=PipetaskCommand)
        @opt.run_options()
        @opt.config_search_path_option()
        @opt.no_existing_outputs_option()
        def fake_run(ctx: click.Context, **kwargs: object):
            kwargs = collect_pipeline_actions(ctx, **kwargs)
            mock(**kwargs)

        # At least one tests requires that we enable INFO logging so use
        # the specialist runner.
        runner = LogCliRunner()
        result = runner.invoke(fake_run, args, catch_exceptions=False)
        if result.exit_code != 0:
            raise RuntimeError(f"Failure getting default args for 'run': {result}")
        mock.assert_called_once()
        result: dict[str, object] = mock.call_args[1]
        result["show"] = ShowInfo([])
        result.update(kwargs)
        return result

    def test_missing_options(self):
        """Test that if options for the run script are missing that it
        fails.
        """

        @click.command()
        @opt.pipeline_build_options()
        def cli(**kwargs):
            script.run(**kwargs)

        runner = click.testing.CliRunner()
        result = runner.invoke(cli)
        # The cli call should fail, because qgraph.run takes more options
        # than are defined by pipeline_build_options.
        self.assertNotEqual(result.exit_code, 0)

    def test_simple_qg(self):
        """Test execution of a trivial quantum graph."""
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg = script.qgraph(**kwargs)
            self.assertEqual(len(qg.quanta_by_task), 2)
            self.assertEqual(len(qg), 2)
            # Ensure that the output run used in the graph is also used in the
            # pipeline execution. It is possible for 'qgraph' and 'run' to
            # calculate time-stamped runs across a second boundary.
            kwargs["output_run"] = qg.header.output_run
            # Execute the graph and check for output existence.
            script.run(qg, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=["output"]).count(), 1)
            # Test that we've disabled implicit threading
            self.assertEqual(os.environ["OMP_NUM_THREADS"], "1")

    def test_simple_qg_rebase(self):
        """Test execution of a trivial quantum graph, with --rebase used to
        force redefinition of the output collection.
        """
        with DirectButlerRepo.make_temporary(input_chain="test1") as (helper, root):
            helper.add_task()
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            # Pass one input collection here for the usual test setup; we'll
            # override it later.
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            # We'll actually pass two input collections in.  One is empty, but
            # the stuff we're testing here doesn't care.
            kwargs["input"] = ["test2", "test1"]
            helper.butler.collections.register("test2", CollectionType.RUN)
            # Set up the output collection with a sequence that doesn't end the
            # same way as the input collection.  This is normally an error.
            helper.butler.collections.register("output", CollectionType.CHAINED)
            helper.butler.collections.register("unexpected_input", CollectionType.RUN)
            helper.butler.collections.register("output/run0", CollectionType.RUN)
            helper.butler.collections.redefine_chain(
                "output", ["test2", "unexpected_input", "test1", "output/run0"]
            )
            # Without --rebase, the inconsistent input and output collections
            # are an error.
            with self.assertRaises(ValueError):
                script.qgraph(**kwargs)
            # With --rebase, the output collection gets redefined.
            kwargs["rebase"] = True
            qg = script.qgraph(**kwargs)
            self.assertEqual(len(qg.quanta_by_task), 2)
            self.assertEqual(len(qg), 2)
            # Ensure that the output run used in the graph is also used in the
            # pipeline execution.
            kwargs["output_run"] = qg.header.output_run
            # Execute the graph and check for output existence.
            script.run(qg, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=["output"]).count(), 1)

    def test_simple_qgraph_qbb(self):
        """Test execution of a trivial quantum graph in QBB mode."""
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            # It's unusual to put a QG in a butler root, but since we've
            # already got a temp dir, we might as well use it.
            qg_file_1 = os.path.join(root, "test1.qg")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                "--qgraph-datastore-records",
                "--save-qgraph",
                qg_file_1,
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg = script.qgraph(**kwargs)
            output_run = qg.header.output_run
            output = qg.header.output
            self.assertEqual(len(qg.quanta_by_task), 2)
            self.assertEqual(len(qg), 2)
            # Execute with QBB.
            kwargs.update(output_run=output_run, qgraph=qg_file_1)
            script.pre_exec_init_qbb(**kwargs)
            script.run_qbb(**kwargs)
            # Transfer the datasets to the butler.
            n1 = transfer_from_graph(
                qg_file_1,
                root,
                register_dataset_types=True,
                transfer_dimensions=False,
                update_output_chain=True,
                dry_run=False,
                dataset_type=(),
            )
            self.assertEqual(n1, 9)
            # Check that the expected outputs exist.
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=output).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=output).count(), 1)
            # Check that some metadata keys were written.
            some_task_label = next(iter(qg.pipeline_graph.tasks))
            (some_metadata_ref,) = helper.butler.query_datasets(
                f"{some_task_label}_metadata",
                limit=1,
                collections=output,
            )
            some_metadata = helper.butler.get(some_metadata_ref)
            self.assertIn("qg_read_time", some_metadata["job"])
            self.assertIn("qg_size", some_metadata["job"])

            # Update the output run and try again.
            new_output_run = output_run + "_new"
            qg_file_2 = os.path.join(root, "test2.qg")
            script.update_graph_run(qg_file_1, new_output_run, qg_file_2)
            kwargs.update(qgraph=qg_file_2)
            # Execute with QBB again.
            script.pre_exec_init_qbb(**kwargs)
            script.run_qbb(**kwargs)
            # Transfer the datasets to the butler.
            n2 = transfer_from_graph(
                qg_file_2,
                root,
                register_dataset_types=True,
                transfer_dimensions=False,
                update_output_chain=False,
                dry_run=False,
                dataset_type=(),
            )
            self.assertEqual(n2, 9)
            # Check that the expected outputs exist in the new run.
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=new_output_run).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=new_output_run).count(), 1)

    def test_empty_qg(self):
        """Test that making an empty QG produces the right error messages."""
        with DirectButlerRepo.make_temporary("base.yaml") as (helper, root):
            helper.add_task(dimensions=["instrument"])
            helper.add_task(dimensions=["instrument"])
            helper.pipeline_graph.resolve(registry=helper.butler.registry)
            helper.butler.registry.registerDatasetType(
                helper.pipeline_graph.dataset_types["dataset_auto0"].dataset_type
            )
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            # Note that we haven't inserted any datasets into this repo; that's
            # how we'll force an empty graph.
            with self.assertLogs(level=logging.ERROR) as cm:
                qg = script.qgraph(**kwargs)
            self.assertRegex(
                cm.output[0], ".*Initial data ID query returned no rows, so QuantumGraph will be empty.*"
            )
            self.assertRegex(cm.output[0], ".*dataset_auto0.*input_run.*doomed to fail.")
            self.assertIsNone(qg)

    def test_simple_qg_no_skip_existing_inputs(self):
        """Test for case when output data for one task already appears in
        the *input* collection, but no ``--extend-run`` or ``-skip-existing``
        option is present.
        """
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg1 = script.qgraph(**kwargs)
            run1 = qg1.header.output_run
            self.assertEqual(len(qg1.quanta_by_task["task_auto1"]), 1)
            self.assertEqual(len(qg1.quanta_by_task["task_auto2"]), 1)
            self.assertEqual(len(qg1), 2)
            # Ensure that the output run used in the graph is also used in the
            # pipeline execution. It is possible for 'qgraph' and 'run' to
            # calculate time-stamped runs across a second boundary.
            kwargs["output_run"] = run1
            # Execute the graph and check for output existence.
            script.run(qg1, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=["output"]).count(), 1)
            # Make a new QG with the same output collection, but a new RUN
            # collection, it should run again, shadowing the previous outputs.
            kwargs["output_run"] = None
            time.sleep(1)  # Make sure we don't get the same RUN timestamp.
            qg2 = script.qgraph(**kwargs)
            run2 = qg2.header.output_run
            self.assertNotEqual(run1, run2)
            self.assertEqual(len(qg1.quanta_by_task["task_auto1"]), 1)
            self.assertEqual(len(qg1.quanta_by_task["task_auto2"]), 1)
            self.assertEqual(len(qg2), 2)
            kwargs["output_run"] = run2
            script.run(qg2, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=[run2]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=[run2]).count(), 1)

    def test_simple_qg_skip_existing_inputs(self):
        """Test for case when output data for one task already appears in
        the *input* collection, but no ``--extend-run`` or ``-skip-existing``
        option is present.
        """
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg1 = script.qgraph(**kwargs)
            run1 = qg1.header.output_run
            self.assertEqual(len(qg1.quanta_by_task["task_auto1"]), 1)
            self.assertEqual(len(qg1), 1)
            # Ensure that the output run used in the graph is also used in the
            # pipeline execution. It is possible for 'qgraph' and 'run' to
            # calculate time-stamped runs across a second boundary.
            kwargs["output_run"] = run1
            # Execute the graph and check for output existence.
            script.run(qg1, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
            # Make a new QG with the same output collection, but a new RUN
            # collection, with --skip-existing-in, and one more task.  The
            # first task should be skipped and the second should be run.
            helper.add_task()
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                "--skip-existing-in",
                "output",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            time.sleep(1)  # Make sure we don't get the same RUN timestamp.
            qg2 = script.qgraph(**kwargs)
            run2 = qg2.header.output_run
            self.assertNotEqual(run1, run2)
            self.assertEqual(len(qg2.quanta_by_task["task_auto1"]), 0)
            self.assertEqual(len(qg2.quanta_by_task["task_auto2"]), 1)
            self.assertEqual(len(qg2), 1)
            kwargs["output_run"] = run2
            script.run(qg2, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=[run2]).count(), 0)
                self.assertEqual(query.datasets("dataset_auto2", collections=[run2]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=["output"]).count(), 1)

    def test_simple_qg_extend_run(self):
        """Test for case when output data for one task already appears in
        the output RUN collection, and `--extend-run` is used to skip it.
        """
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg1 = script.qgraph(**kwargs)
            run1 = qg1.header.output_run
            self.assertEqual(len(qg1.quanta_by_task["task_auto1"]), 1)
            self.assertEqual(len(qg1), 1)
            # Ensure that the output run used in the graph is also used in the
            # pipeline execution. It is possible for 'qgraph' and 'run' to
            # calculate time-stamped runs across a second boundary.
            kwargs["output_run"] = run1
            # Execute the graph and check for output existence.
            script.run(qg1, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
            # Make a new QG with the same output collection, but a new RUN
            # collection, with --extend-run, and one more task.  The first task
            # should be skipped and the second should be run.
            helper.add_task()
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                "--extend-run",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg2 = script.qgraph(**kwargs)
            run2 = qg2.header.output_run
            self.assertEqual(run1, run2)
            self.assertEqual(len(qg2.quanta_by_task["task_auto1"]), 0)
            self.assertEqual(len(qg2.quanta_by_task["task_auto2"]), 1)
            self.assertEqual(len(qg2), 1)
            kwargs["output_run"] = run2
            script.run(qg2, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=[run2]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=[run2]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=["output"]).count(), 1)

    def test_simple_qg_clobber(self):
        """Test for case when output data for one task already appears in
        the output RUN collection, and `--extend-run --clobber-outputs` is used
        to skip it.
        """
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg1 = script.qgraph(**kwargs)
            run1 = qg1.header.output_run
            self.assertEqual(len(qg1.quanta_by_task), 1)
            self.assertEqual(len(qg1), 1)
            # Ensure that the output run used in the graph is also used in the
            # pipeline execution. It is possible for 'qgraph' and 'run' to
            # calculate time-stamped runs across a second boundary.
            kwargs["output_run"] = run1
            # Execute the graph and check for output existence.
            script.run(qg1, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
            # Delete the metadata output so we don't take the skip-existing
            # logic path instead of the clobbering one.
            helper.butler.pruneDatasets(
                helper.butler.query_datasets("task_auto1_metadata", collections=run1),
                purge=True,
                unstore=True,
                disassociate=True,
            )
            # Make a new QG with the same output collection, but a new RUN
            # collection, with --clobber-outputs, and one more task.  Both
            # tasks should be run.
            helper.add_task()
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                "--extend-run",
                "--clobber-outputs",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg2 = script.qgraph(**kwargs)
            run2 = qg2.header.output_run
            self.assertEqual(run1, run2)
            self.assertEqual(len(qg2.quanta_by_task), 2)
            self.assertEqual(len(qg2), 2)
            kwargs["output_run"] = run2
            script.run(qg2, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=[run2]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=[run2]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto2", collections=["output"]).count(), 1)

    def test_simple_qg_replace_run(self):
        """Test repeated execution of a trivial quantum graph with
        --replace-run.
        """
        with DirectButlerRepo.make_temporary() as (helper, root):
            helper.add_task()
            helper.insert_datasets("dataset_auto0")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg1 = script.qgraph(**kwargs)
            run1 = qg1.header.output_run
            self.assertEqual(len(qg1.quanta_by_task["task_auto1"]), 1)
            self.assertEqual(len(qg1), 1)
            # Ensure that the output run used in the graph is also used in the
            # pipeline execution. It is possible for 'qgraph' and 'run' to
            # calculate time-stamped runs across a second boundary.
            kwargs["output_run"] = run1
            # Execute the graph and check for output existence.
            script.run(qg1, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
            # Delete the metadata output so we don't take the skip-existing
            # logic path instead of the clobbering one.
            helper.butler.pruneDatasets(
                helper.butler.query_datasets("task_auto1_metadata", collections=run1),
                purge=True,
                unstore=True,
                disassociate=True,
            )
            # Make a new QG with the same output collection, but a new RUN
            # collection, with --clobber-outputs, and one more task.  Both
            # tasks should be run.
            time.sleep(1)  # Make sure we don't get the same RUN timestamp.
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--replace-run",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg2 = script.qgraph(**kwargs)
            run2 = qg2.header.output_run
            self.assertNotEqual(run1, run2)
            self.assertEqual(len(qg2.quanta_by_task["task_auto1"]), 1)
            self.assertEqual(len(qg2), 1)
            kwargs["output_run"] = run2
            script.run(qg2, **kwargs)
            self.assertNotIn(run1, helper.butler.collections.get_info("output").children)
            self.assertIn(run2, helper.butler.collections.get_info("output").children)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=[run2]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
            # Repeat once again with --prune-replaced as well.
            time.sleep(1)  # Make sure we don't get the same RUN timestamp.
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--replace-run",
                "--prune-replaced",
                "purge",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg3 = script.qgraph(**kwargs)
            run3 = qg3.header.output_run
            self.assertNotEqual(run2, run3)
            self.assertEqual(len(qg3.quanta_by_task["task_auto1"]), 1)
            self.assertEqual(len(qg3), 1)
            kwargs["output_run"] = run3
            script.run(qg3, **kwargs)
            self.assertNotIn(run2, helper.butler.collections.get_info("output").children)
            with self.assertRaises(MissingCollectionError):
                helper.butler.collections.get_info(run2)
            self.assertIn(run3, helper.butler.collections.get_info("output").children)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=[run3]).count(), 1)
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 1)
            # Trying to run again with inputs that aren't exactly what we
            # started with is an error, and the kind that should not modify the
            # data repo.
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                run1,
                "-o",
                "output",
                "--replace-run",
                "--prune-replaced",
                "purge",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            with self.assertRaises(ValueError):
                script.qgraph(**kwargs)

    def test_qg_partial_failure(self):
        """Test execution of a quantum graph where one quantum fails but others
        should continue.
        """
        with DirectButlerRepo.make_temporary("base.yaml") as (helper, root):
            helper.add_task(
                dimensions=["detector"], config=DynamicTestPipelineTaskConfig(fail_condition="detector=3")
            )
            helper.insert_datasets("dataset_auto0")
            kwargs = self._make_run_args(
                "-b",
                root,
                "-i",
                helper.input_chain,
                "-o",
                "output",
                "--register-dataset-types",
                pipeline_graph_factory=PipelineGraphFactory(pipeline_graph=helper.pipeline_graph),
            )
            qg = script.qgraph(**kwargs)
            self.assertEqual(len(qg.quanta_by_task), 1)
            self.assertEqual(len(qg), 4)
            kwargs["output_run"] = qg.header.output_run
            # Execute the graph and check for output existence.
            with self.assertRaises(MPGraphExecutorError):
                script.run(qg, **kwargs)
            with helper.butler.query() as query:
                self.assertEqual(query.datasets("dataset_auto1", collections=["output"]).count(), 3)


class CoverageTestCase(unittest.TestCase):
    """Test the coverage context manager."""

    @unittest.mock.patch.dict("sys.modules", coverage=unittest.mock.MagicMock())
    def testWithCoverage(self):
        """Test that the coverage context manager runs when invoked."""
        with coverage_context({"coverage": True}):
            self.assertTrue(True)

    @unittest.mock.patch("lsst.ctrl.mpexec.cli.cmd.commands.import_module", side_effect=ModuleNotFoundError())
    def testWithMissingCoverage(self, mock_import):  # numpydoc ignore=PR01
        """Test that the coverage context manager complains when coverage is
        not available.
        """
        with self.assertRaises(click.exceptions.ClickException):
            with coverage_context({"coverage": True}):
                pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
