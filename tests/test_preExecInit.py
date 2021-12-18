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

"""Simple unit test for PreExecInit class.
"""

import contextlib
import shutil
import tempfile
import unittest

from lsst.ctrl.mpexec import PreExecInit
from lsst.pipe.base.tests.simpleQGraph import AddTaskFactoryMock, makeSimpleQGraph


@contextlib.contextmanager
def temporaryDirectory():
    """Context manager that creates and destroys temporary directory.

    Difference from `tempfile.TemporaryDirectory` is that it ignores errors
    when deleting a directory, which may happen with some filesystems.
    """
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


class PreExecInitTestCase(unittest.TestCase):
    """A test case for PreExecInit"""

    def test_saveInitOutputs(self):
        taskFactory = AddTaskFactoryMock()
        for extendRun in (False, True):
            with self.subTest(extendRun=extendRun):
                with temporaryDirectory() as tmpdir:
                    butler, qgraph = makeSimpleQGraph(root=tmpdir)
                    preExecInit = PreExecInit(butler=butler, taskFactory=taskFactory, extendRun=extendRun)
                    preExecInit.saveInitOutputs(qgraph)

    def test_saveInitOutputs_twice(self):
        taskFactory = AddTaskFactoryMock()
        for extendRun in (False, True):
            with self.subTest(extendRun=extendRun):
                with temporaryDirectory() as tmpdir:
                    butler, qgraph = makeSimpleQGraph(root=tmpdir)
                    preExecInit = PreExecInit(butler=butler, taskFactory=taskFactory, extendRun=extendRun)
                    preExecInit.saveInitOutputs(qgraph)
                    if extendRun:
                        # will ignore this
                        preExecInit.saveInitOutputs(qgraph)
                    else:
                        # Second time it will fail
                        with self.assertRaises(Exception):
                            preExecInit.saveInitOutputs(qgraph)

    def test_saveConfigs(self):
        for extendRun in (False, True):
            with self.subTest(extendRun=extendRun):
                with temporaryDirectory() as tmpdir:
                    butler, qgraph = makeSimpleQGraph(root=tmpdir)
                    preExecInit = PreExecInit(butler=butler, taskFactory=None, extendRun=extendRun)
                    preExecInit.saveConfigs(qgraph)

    def test_saveConfigs_twice(self):
        for extendRun in (False, True):
            with self.subTest(extendRun=extendRun):
                with temporaryDirectory() as tmpdir:
                    butler, qgraph = makeSimpleQGraph(root=tmpdir)
                    preExecInit = PreExecInit(butler=butler, taskFactory=None, extendRun=extendRun)
                    preExecInit.saveConfigs(qgraph)
                    if extendRun:
                        # will ignore this
                        preExecInit.saveConfigs(qgraph)
                    else:
                        # Second time it will fail
                        with self.assertRaises(Exception):
                            preExecInit.saveConfigs(qgraph)

    def test_savePackageVersions(self):
        for extendRun in (False, True):
            with self.subTest(extendRun=extendRun):
                with temporaryDirectory() as tmpdir:
                    butler, qgraph = makeSimpleQGraph(root=tmpdir)
                    preExecInit = PreExecInit(butler=butler, taskFactory=None, extendRun=extendRun)
                    preExecInit.savePackageVersions(qgraph)

    def test_savePackageVersions_twice(self):
        for extendRun in (False, True):
            with self.subTest(extendRun=extendRun):
                with temporaryDirectory() as tmpdir:
                    butler, qgraph = makeSimpleQGraph(root=tmpdir)
                    preExecInit = PreExecInit(butler=butler, taskFactory=None, extendRun=extendRun)
                    preExecInit.savePackageVersions(qgraph)
                    if extendRun:
                        # If this is the same packages then it should not
                        # attempt to save.
                        preExecInit.savePackageVersions(qgraph)
                    else:
                        # second time it will fail
                        with self.assertRaises(Exception):
                            preExecInit.savePackageVersions(qgraph)


if __name__ == "__main__":
    unittest.main()
