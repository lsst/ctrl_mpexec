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
import lsst.utils.tests
from lsst.ctrl.mpexec import SeparablePipelineExecutor
from lsst.pipe.base import Instrument

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SeparablePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SeparablePipelineExecutor API with a trivial task."""

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

        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "input",
                dimensions=self.butler.registry.dimensions.empty,
                storageClass="StructuredDataDict",
            )
        )
        self.butler.put({"zero": 0}, "input")

    def test_init_badinput(self):
        butler = lsst.daf.butler.Butler(butler=self.butler, collections=[], run="foo")

        with self.assertRaises(ValueError):
            SeparablePipelineExecutor(butler)

    def test_init_badoutput(self):
        butler = lsst.daf.butler.Butler(butler=self.butler, collections=["foo"])

        with self.assertRaises(ValueError):
            SeparablePipelineExecutor(butler)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
