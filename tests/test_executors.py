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

"""Simple unit test for cmdLineFwk module.
"""

import faulthandler
import logging
import os
import signal
import sys
import time
import unittest
import warnings
from multiprocessing import Manager

import networkx as nx
import psutil
from lsst.ctrl.mpexec import (
    ExecutionStatus,
    MPGraphExecutor,
    MPGraphExecutorError,
    MPTimeoutError,
    QuantumExecutor,
    QuantumReport,
    SingleQuantumExecutor,
)
from lsst.ctrl.mpexec.execFixupDataId import ExecFixupDataId
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base import NodeId
from lsst.pipe.base.tests.simpleQGraph import AddTaskFactoryMock, makeSimpleQGraph

logging.basicConfig(level=logging.DEBUG)

_LOG = logging.getLogger(__name__)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QuantumExecutorMock(QuantumExecutor):
    """Mock class for QuantumExecutor"""

    def __init__(self, mp=False):
        self.quanta = []
        if mp:
            # in multiprocess mode use shared list
            manager = Manager()
            self.quanta = manager.list()
        self.report = None
        self._execute_called = False

    def execute(self, taskDef, quantum):
        _LOG.debug("QuantumExecutorMock.execute: taskDef=%s dataId=%s", taskDef, quantum.dataId)
        self._execute_called = True
        if taskDef.taskClass:
            try:
                # only works for one of the TaskMock classes below
                taskDef.taskClass().runQuantum()
                self.report = QuantumReport(dataId=quantum.dataId, taskLabel=taskDef.label)
            except Exception as exc:
                self.report = QuantumReport.from_exception(
                    exception=exc,
                    dataId=quantum.dataId,
                    taskLabel=taskDef.label,
                )
                raise
        self.quanta.append(quantum)
        return quantum

    def getReport(self):
        if not self._execute_called:
            raise RuntimeError("getReport called before execute")
        return self.report

    def getDataIds(self, field):
        """Return values for dataId field for each visited quanta."""
        return [quantum.dataId[field] for quantum in self.quanta]


class QuantumMock:
    """Mock equivalent of a `~lsst.daf.butler.Quantum`."""

    def __init__(self, dataId):
        self.dataId = dataId

    def __eq__(self, other):
        return self.dataId == other.dataId

    def __hash__(self):
        # dict.__eq__ is order-insensitive
        return hash(tuple(sorted(kv for kv in self.dataId.items())))


class QuantumIterDataMock:
    """Simple class to mock QuantumIterData."""

    def __init__(self, index, taskDef, **dataId):
        self.index = index
        self.taskDef = taskDef
        self.quantum = QuantumMock(dataId)
        self.dependencies = set()
        self.nodeId = NodeId(index, "DummyBuildString")


class QuantumGraphMock:
    """Mock for quantum graph."""

    def __init__(self, qdata):
        self._graph = nx.DiGraph()
        previous = qdata[0]
        for node in qdata[1:]:
            self._graph.add_edge(previous, node)
            previous = node

    def __iter__(self):
        yield from nx.topological_sort(self._graph)

    def __len__(self):
        return len(self._graph)

    def findTaskDefByLabel(self, label):
        for q in self:
            if q.taskDef.label == label:
                return q.taskDef

    def getQuantaForTask(self, taskDef):
        nodes = self.getNodesForTask(taskDef)
        return {q.quantum for q in nodes}

    def getNodesForTask(self, taskDef):
        quanta = set()
        for q in self:
            if q.taskDef == taskDef:
                quanta.add(q)
        return quanta

    @property
    def graph(self):
        return self._graph

    def findCycle(self):
        return []

    def determineInputsToQuantumNode(self, node):
        result = set()
        for n in node.dependencies:
            for otherNode in self:
                if otherNode.index == n:
                    result.add(otherNode)
        return result


class TaskMockMP:
    """Simple mock class for task supporting multiprocessing."""

    canMultiprocess = True

    def runQuantum(self):
        _LOG.debug("TaskMockMP.runQuantum")
        pass


class TaskMockFail:
    """Simple mock class for task which fails."""

    canMultiprocess = True

    def runQuantum(self):
        _LOG.debug("TaskMockFail.runQuantum")
        raise ValueError("expected failure")


class TaskMockCrash:
    """Simple mock class for task which fails."""

    canMultiprocess = True

    def runQuantum(self):
        _LOG.debug("TaskMockCrash.runQuantum")
        # Disable fault handler to suppress long scary traceback.
        faulthandler.disable()
        signal.raise_signal(signal.SIGILL)


class TaskMockLongSleep:
    """Simple mock class for task which "runs" for very long time."""

    canMultiprocess = True

    def runQuantum(self):
        _LOG.debug("TaskMockLongSleep.runQuantum")
        time.sleep(100.0)


class TaskMockNoMP:
    """Simple mock class for task not supporting multiprocessing."""

    canMultiprocess = False


class TaskDefMock:
    """Simple mock class for task definition in a pipeline."""

    def __init__(self, taskName="Task", config=None, taskClass=TaskMockMP, label="task1"):
        self.taskName = taskName
        self.config = config
        self.taskClass = taskClass
        self.label = label

    def __str__(self):
        return f"TaskDefMock(taskName={self.taskName}, taskClass={self.taskClass.__name__})"


def _count_status(report, status):
    """Count number of quanta witha a given status."""
    return len([qrep for qrep in report.quantaReports if qrep.status is status])


class MPGraphExecutorTestCase(unittest.TestCase):
    """A test case for MPGraphExecutor class"""

    def test_mpexec_nomp(self):
        """Make simple graph and execute"""
        taskDef = TaskDefMock()
        qgraph = QuantumGraphMock(
            [QuantumIterDataMock(index=i, taskDef=taskDef, detector=i) for i in range(3)]
        )

        # run in single-process mode
        qexec = QuantumExecutorMock()
        mpexec = MPGraphExecutor(numProc=1, timeout=100, quantumExecutor=qexec)
        mpexec.execute(qgraph)
        self.assertEqual(qexec.getDataIds("detector"), [0, 1, 2])
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.SUCCESS)
        self.assertIsNone(report.exitCode)
        self.assertIsNone(report.exceptionInfo)
        self.assertEqual(len(report.quantaReports), 3)
        self.assertTrue(all(qrep.status == ExecutionStatus.SUCCESS for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exitCode is None for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))
        self.assertTrue(all(qrep.taskLabel == "task1" for qrep in report.quantaReports))

    def test_mpexec_mp(self):
        """Make simple graph and execute"""
        taskDef = TaskDefMock()
        qgraph = QuantumGraphMock(
            [QuantumIterDataMock(index=i, taskDef=taskDef, detector=i) for i in range(3)]
        )

        methods = ["spawn"]
        if sys.platform == "linux":
            methods.append("fork")
            methods.append("forkserver")

        for method in methods:
            with self.subTest(startMethod=method):
                # Run in multi-process mode, the order of results is not
                # defined.
                qexec = QuantumExecutorMock(mp=True)
                mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec, startMethod=method)
                mpexec.execute(qgraph)
                self.assertCountEqual(qexec.getDataIds("detector"), [0, 1, 2])
                report = mpexec.getReport()
                self.assertEqual(report.status, ExecutionStatus.SUCCESS)
                self.assertIsNone(report.exitCode)
                self.assertIsNone(report.exceptionInfo)
                self.assertEqual(len(report.quantaReports), 3)
                self.assertTrue(all(qrep.status == ExecutionStatus.SUCCESS for qrep in report.quantaReports))
                self.assertTrue(all(qrep.exitCode == 0 for qrep in report.quantaReports))
                self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))
                self.assertTrue(all(qrep.taskLabel == "task1" for qrep in report.quantaReports))

    def test_mpexec_nompsupport(self):
        """Try to run MP for task that has no MP support which should fail"""
        taskDef = TaskDefMock(taskClass=TaskMockNoMP)
        qgraph = QuantumGraphMock(
            [QuantumIterDataMock(index=i, taskDef=taskDef, detector=i) for i in range(3)]
        )

        # run in multi-process mode
        qexec = QuantumExecutorMock()
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "Task Task does not support multiprocessing"):
            mpexec.execute(qgraph)

    def test_mpexec_fixup(self):
        """Make simple graph and execute, add dependencies by executing fixup
        code.
        """
        taskDef = TaskDefMock()

        for reverse in (False, True):
            qgraph = QuantumGraphMock(
                [QuantumIterDataMock(index=i, taskDef=taskDef, detector=i) for i in range(3)]
            )

            qexec = QuantumExecutorMock()
            fixup = ExecFixupDataId("task1", "detector", reverse=reverse)
            mpexec = MPGraphExecutor(numProc=1, timeout=100, quantumExecutor=qexec, executionGraphFixup=fixup)
            mpexec.execute(qgraph)

            expected = [0, 1, 2]
            if reverse:
                expected = list(reversed(expected))
            self.assertEqual(qexec.getDataIds("detector"), expected)

    def test_mpexec_timeout(self):
        """Fail due to timeout"""
        taskDef = TaskDefMock()
        taskDefSleep = TaskDefMock(taskClass=TaskMockLongSleep)
        qgraph = QuantumGraphMock(
            [
                QuantumIterDataMock(index=0, taskDef=taskDef, detector=0),
                QuantumIterDataMock(index=1, taskDef=taskDefSleep, detector=1),
                QuantumIterDataMock(index=2, taskDef=taskDef, detector=2),
            ]
        )

        # with failFast we'll get immediate MPTimeoutError
        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=1, quantumExecutor=qexec, failFast=True)
        with self.assertRaises(MPTimeoutError):
            mpexec.execute(qgraph)
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.TIMEOUT)
        self.assertEqual(report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPTimeoutError")
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.TIMEOUT), 1)
        self.assertTrue(any(qrep.exitCode < 0 for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

        # with failFast=False exception happens after last task finishes
        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=3, quantumExecutor=qexec, failFast=False)
        with self.assertRaises(MPTimeoutError):
            mpexec.execute(qgraph)
        # We expect two tasks (0 and 2) to finish successfully and one task to
        # timeout. Unfortunately on busy CPU there is no guarantee that tasks
        # finish on time, so expect more timeouts and issue a warning.
        detectorIds = set(qexec.getDataIds("detector"))
        self.assertLess(len(detectorIds), 3)
        if detectorIds != {0, 2}:
            warnings.warn(f"Possibly timed out tasks, expected [0, 2], received {detectorIds}")
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.TIMEOUT)
        self.assertEqual(report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPTimeoutError")
        self.assertGreater(len(report.quantaReports), 0)
        self.assertGreater(_count_status(report, ExecutionStatus.TIMEOUT), 0)
        self.assertTrue(any(qrep.exitCode < 0 for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_failure(self):
        """Failure in one task should not stop other tasks"""
        taskDef = TaskDefMock()
        taskDefFail = TaskDefMock(taskClass=TaskMockFail)
        qgraph = QuantumGraphMock(
            [
                QuantumIterDataMock(index=0, taskDef=taskDef, detector=0),
                QuantumIterDataMock(index=1, taskDef=taskDefFail, detector=1),
                QuantumIterDataMock(index=2, taskDef=taskDef, detector=2),
            ]
        )

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)
        self.assertCountEqual(qexec.getDataIds("detector"), [0, 2])
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertTrue(any(qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_dep(self):
        """Failure in one task should skip dependents"""
        taskDef = TaskDefMock()
        taskDefFail = TaskDefMock(taskClass=TaskMockFail)
        qdata = [
            QuantumIterDataMock(index=0, taskDef=taskDef, detector=0),
            QuantumIterDataMock(index=1, taskDef=taskDefFail, detector=1),
            QuantumIterDataMock(index=2, taskDef=taskDef, detector=2),
            QuantumIterDataMock(index=3, taskDef=taskDef, detector=3),
            QuantumIterDataMock(index=4, taskDef=taskDef, detector=4),
        ]
        qdata[2].dependencies.add(1)
        qdata[4].dependencies.add(3)
        qdata[4].dependencies.add(2)

        qgraph = QuantumGraphMock(qdata)

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)
        self.assertCountEqual(qexec.getDataIds("detector"), [0, 3])
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertEqual(_count_status(report, ExecutionStatus.SKIPPED), 2)
        self.assertTrue(any(qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_dep_nomp(self):
        """Failure in one task should skip dependents, in-process version"""
        taskDef = TaskDefMock()
        taskDefFail = TaskDefMock(taskClass=TaskMockFail)
        qdata = [
            QuantumIterDataMock(index=0, taskDef=taskDef, detector=0),
            QuantumIterDataMock(index=1, taskDef=taskDefFail, detector=1),
            QuantumIterDataMock(index=2, taskDef=taskDef, detector=2),
            QuantumIterDataMock(index=3, taskDef=taskDef, detector=3),
            QuantumIterDataMock(index=4, taskDef=taskDef, detector=4),
        ]
        qdata[2].dependencies.add(1)
        qdata[4].dependencies.add(3)
        qdata[4].dependencies.add(2)

        qgraph = QuantumGraphMock(qdata)

        qexec = QuantumExecutorMock()
        mpexec = MPGraphExecutor(numProc=1, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)
        self.assertCountEqual(qexec.getDataIds("detector"), [0, 3])
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertEqual(_count_status(report, ExecutionStatus.SKIPPED), 2)
        self.assertTrue(all(qrep.exitCode is None for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_failfast(self):
        """Fast fail stops quickly.

        Timing delay of task #3 should be sufficient to process
        failure and raise exception.
        """
        taskDef = TaskDefMock()
        taskDefFail = TaskDefMock(taskClass=TaskMockFail)
        taskDefLongSleep = TaskDefMock(taskClass=TaskMockLongSleep)
        qdata = [
            QuantumIterDataMock(index=0, taskDef=taskDef, detector=0),
            QuantumIterDataMock(index=1, taskDef=taskDefFail, detector=1),
            QuantumIterDataMock(index=2, taskDef=taskDef, detector=2),
            QuantumIterDataMock(index=3, taskDef=taskDefLongSleep, detector=3),
            QuantumIterDataMock(index=4, taskDef=taskDef, detector=4),
        ]
        qdata[1].dependencies.add(0)
        qdata[2].dependencies.add(1)
        qdata[4].dependencies.add(3)
        qdata[4].dependencies.add(2)

        qgraph = QuantumGraphMock(qdata)

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec, failFast=True)
        with self.assertRaisesRegex(MPGraphExecutorError, "failed, exit code=1"):
            mpexec.execute(qgraph)
        self.assertCountEqual(qexec.getDataIds("detector"), [0])
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertTrue(any(qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_crash(self):
        """Check task crash due to signal"""
        taskDef = TaskDefMock()
        taskDefCrash = TaskDefMock(taskClass=TaskMockCrash)
        qgraph = QuantumGraphMock(
            [
                QuantumIterDataMock(index=0, taskDef=taskDef, detector=0),
                QuantumIterDataMock(index=1, taskDef=taskDefCrash, detector=1),
                QuantumIterDataMock(index=2, taskDef=taskDef, detector=2),
            ]
        )

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertTrue(any(qrep.exitCode == -signal.SIGILL for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_crash_failfast(self):
        """Check task crash due to signal with --fail-fast"""
        taskDef = TaskDefMock()
        taskDefCrash = TaskDefMock(taskClass=TaskMockCrash)
        qgraph = QuantumGraphMock(
            [
                QuantumIterDataMock(index=0, taskDef=taskDef, detector=0),
                QuantumIterDataMock(index=1, taskDef=taskDefCrash, detector=1),
                QuantumIterDataMock(index=2, taskDef=taskDef, detector=2),
            ]
        )

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec, failFast=True)
        with self.assertRaisesRegex(MPGraphExecutorError, "failed, killed by signal 4 .Illegal instruction"):
            mpexec.execute(qgraph)
        report = mpexec.getReport()
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertTrue(any(qrep.exitCode == -signal.SIGILL for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_num_fd(self):
        """Check that number of open files stays reasonable"""
        taskDef = TaskDefMock()
        qgraph = QuantumGraphMock(
            [QuantumIterDataMock(index=i, taskDef=taskDef, detector=i) for i in range(20)]
        )

        this_proc = psutil.Process()
        num_fds_0 = this_proc.num_fds()

        # run in multi-process mode, the order of results is not defined
        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        mpexec.execute(qgraph)

        num_fds_1 = this_proc.num_fds()
        # They should be the same but allow small growth just in case.
        # Without DM-26728 fix the difference would be equal to number of
        # quanta (20).
        self.assertLess(num_fds_1 - num_fds_0, 5)


class SingleQuantumExecutorTestCase(unittest.TestCase):
    """Tests for SingleQuantumExecutor implementation."""

    instrument = "lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.root)

    def test_simple_execute(self) -> None:
        """Run execute() method in simplest setup."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        taskFactory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler, taskFactory)
        executor.execute(node.taskDef, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        # There must be one dataset of task's output connection
        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)

    def test_skip_existing_execute(self) -> None:
        """Run execute() method twice, with skip_existing_in."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        taskFactory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler, taskFactory)
        executor.execute(node.taskDef, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_1 = refs[0].id

        # Re-run it with skipExistingIn, it should not run.
        assert butler.run is not None
        executor = SingleQuantumExecutor(butler, taskFactory, skipExistingIn=[butler.run])
        executor.execute(node.taskDef, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_2 = refs[0].id
        self.assertEqual(dataset_id_1, dataset_id_2)

    def test_clobber_outputs_execute(self) -> None:
        """Run execute() method twice, with clobber_outputs."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        taskFactory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler, taskFactory)
        executor.execute(node.taskDef, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_1 = refs[0].id

        original_dataset = butler.get(refs[0])

        # Remove the dataset ourself, and replace it with something
        # different so we can check later whether it got replaced.
        butler.pruneDatasets([refs[0]], disassociate=False, unstore=True, purge=False)
        replacement = original_dataset + 10
        butler.put(replacement, refs[0])

        # Re-run it with clobberOutputs and skipExistingIn, it should not
        # clobber but should skip instead.
        assert butler.run is not None
        executor = SingleQuantumExecutor(
            butler, taskFactory, skipExistingIn=[butler.run], clobberOutputs=True
        )
        executor.execute(node.taskDef, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_2 = refs[0].id
        self.assertEqual(dataset_id_1, dataset_id_2)

        second_dataset = butler.get(refs[0])
        self.assertEqual(list(second_dataset), list(replacement))

        # Re-run it with clobberOutputs but without skipExistingIn, it should
        # clobber.
        assert butler.run is not None
        executor = SingleQuantumExecutor(butler, taskFactory, clobberOutputs=True)
        executor.execute(node.taskDef, node.quantum)
        self.assertEqual(taskFactory.countExec, 2)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_3 = refs[0].id

        third_dataset = butler.get(refs[0])
        self.assertEqual(list(third_dataset), list(original_dataset))

        # No change in UUID even after replacement
        self.assertEqual(dataset_id_1, dataset_id_3)


if __name__ == "__main__":
    unittest.main()
