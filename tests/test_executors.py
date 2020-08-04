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

"""Simple unit test for cmdLineFwk module.
"""

import logging
import time
import unittest

from lsst.ctrl.mpexec import MPGraphExecutor, MPTimeoutError, QuantumExecutor
from lsst.ctrl.mpexec.execFixupDataId import ExecFixupDataId
from testUtil import makeSimpleQGraph


logging.basicConfig(level=logging.INFO)


class QuantumExecutorMock(QuantumExecutor):
    """Mock class for QuantumExecutor
    """
    def __init__(self, sleep=None):
        self.taskDefs = []
        self.quanta = []
        self.sleep = sleep

    def execute(self, taskDef, quantum, butler):
        self.taskDefs += [taskDef]
        self.quanta += [quantum]
        if self.sleep:
            time.sleep(self.sleep)

    def getDataIds(self, field):
        """Returns values for dataId field for each visited quanta"""
        return [quantum.dataId[field] for quantum in self.quanta]


class MPGraphExecutorTestCase(unittest.TestCase):
    """A test case for MPGraphExecutor class
    """

    def test_mpexec(self):
        """Make simple graph and execute"""

        nQuanta = 3
        butler, qgraph = makeSimpleQGraph(nQuanta)

        qexec = QuantumExecutorMock()
        mpexec = MPGraphExecutor(numProc=1, timeout=1, quantumExecutor=qexec)
        mpexec.execute(qgraph, butler)
        # the order is not defined
        self.assertCountEqual(qexec.getDataIds("detector"), [0, 1, 2])

    def test_mpexec_fixup(self):
        """Make simple graph and execute, add dependencies"""

        nQuanta = 3
        butler, qgraph = makeSimpleQGraph(nQuanta)

        for reverse in (False, True):
            qexec = QuantumExecutorMock()
            fixup = ExecFixupDataId("task1", "detector", reverse=reverse)
            mpexec = MPGraphExecutor(numProc=1, timeout=1, quantumExecutor=qexec,
                                     executionGraphFixup=fixup)
            mpexec.execute(qgraph, butler)

            expected = [0, 1, 2]
            if reverse:
                expected = list(reversed(expected))
            self.assertEqual(qexec.getDataIds("detector"), expected)

    def test_mpexec_timeout(self):
        """Make simple graph and execute, add dependencies"""

        nQuanta = 3
        butler, qgraph = makeSimpleQGraph(nQuanta)

        for reverse in (False, True):
            qexec = QuantumExecutorMock(sleep=3)
            mpexec = MPGraphExecutor(numProc=3, timeout=1, quantumExecutor=qexec)
            with self.assertRaises(MPTimeoutError):
                mpexec.execute(qgraph, butler)


if __name__ == "__main__":
    unittest.main()
