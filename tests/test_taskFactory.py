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

import lsst.daf.butler.tests as butlerTests
import lsst.pex.config as pexConfig
from lsst.ctrl.mpexec import TaskFactory
from lsst.pipe.base import PipelineTaskConfig, PipelineTaskConnections, connectionTypes
from lsst.pipe.base.configOverrides import ConfigOverrides

# Storage class to use for tests of fakes.
_FAKE_STORAGE_CLASS = "StructuredDataDict"


class FakeConnections(PipelineTaskConnections, dimensions=set()):
    initInput = connectionTypes.InitInput(name="fakeInitInput", doc="", storageClass=_FAKE_STORAGE_CLASS)
    initOutput = connectionTypes.InitOutput(name="fakeInitOutput", doc="", storageClass=_FAKE_STORAGE_CLASS)
    input = connectionTypes.Input(
        name="fakeInput", doc="", storageClass=_FAKE_STORAGE_CLASS, dimensions=set()
    )
    output = connectionTypes.Output(
        name="fakeOutput", doc="", storageClass=_FAKE_STORAGE_CLASS, dimensions=set()
    )


class FakeConfig(PipelineTaskConfig, pipelineConnections=FakeConnections):
    widget = pexConfig.Field(dtype=float, doc="")


def mockTaskClass():
    """A class placeholder that records calls to __call__."""
    mock = unittest.mock.Mock(__name__="_TaskMock", _DefaultName="FakeTask", ConfigClass=FakeConfig)
    return mock


class TaskFactoryTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        tmp = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cls.repo = butlerTests.makeTestRepo(tmp)
        butlerTests.addDatasetType(cls.repo, "fakeInitInput", set(), _FAKE_STORAGE_CLASS)
        butlerTests.addDatasetType(cls.repo, "fakeInitOutput", set(), _FAKE_STORAGE_CLASS)
        butlerTests.addDatasetType(cls.repo, "fakeInput", set(), _FAKE_STORAGE_CLASS)
        butlerTests.addDatasetType(cls.repo, "fakeOutput", set(), _FAKE_STORAGE_CLASS)

    def setUp(self):
        super().setUp()

        self.factory = TaskFactory()
        self.constructor = mockTaskClass()

    @staticmethod
    def _alteredConfig():
        config = FakeConfig()
        config.widget = 42.0
        return config

    @staticmethod
    def _overrides():
        overrides = ConfigOverrides()
        overrides.addValueOverride("widget", -1.0)
        return overrides

    @staticmethod
    def _dummyCatalog():
        return {}

    def _tempButler(self):
        butler = butlerTests.makeTestCollection(self.repo, uniqueId=self.id())
        catalog = self._dummyCatalog()
        butler.put(catalog, "fakeInitInput")
        butler.put(catalog, "fakeInitOutput")
        butler.put(catalog, "fakeInput")
        butler.put(catalog, "fakeOutput")
        return butler

    def testOnlyMandatoryArg(self):
        self.factory.makeTask(
            taskClass=self.constructor, label=None, config=None, overrides=None, butler=None
        )
        self.constructor.assert_called_with(config=FakeConfig(), initInputs=None, name=None)

    def testAllArgs(self):
        butler = self._tempButler()
        self.factory.makeTask(
            taskClass=self.constructor,
            label="no-name",
            config=self._alteredConfig(),
            overrides=self._overrides(),
            butler=butler,
        )
        catalog = butler.get("fakeInitInput")  # Copies of _dummyCatalog are identical but not equal
        # When config passed in, overrides ignored
        self.constructor.assert_called_with(
            config=self._alteredConfig(), initInputs={"initInput": catalog}, name="no-name"
        )

    # Can't test all 14 remaining combinations, but the 6 pairs should be
    # enough coverage.

    def testNameConfig(self):
        self.factory.makeTask(
            taskClass=self.constructor,
            label="no-name",
            config=self._alteredConfig(),
            overrides=None,
            butler=None,
        )
        self.constructor.assert_called_with(config=self._alteredConfig(), initInputs=None, name="no-name")

    def testNameOverrides(self):
        self.factory.makeTask(
            taskClass=self.constructor, label="no-name", config=None, overrides=self._overrides(), butler=None
        )
        config = FakeConfig()
        self._overrides().applyTo(config)
        self.constructor.assert_called_with(config=config, initInputs=None, name="no-name")

    def testNameButler(self):
        butler = self._tempButler()
        self.factory.makeTask(
            taskClass=self.constructor, label="no-name", config=None, overrides=None, butler=butler
        )
        catalog = butler.get("fakeInitInput")  # Copies of _dummyCatalog are identical but not equal
        self.constructor.assert_called_with(
            config=FakeConfig(), initInputs={"initInput": catalog}, name="no-name"
        )

    def testConfigOverrides(self):
        self.factory.makeTask(
            taskClass=self.constructor,
            label=None,
            config=self._alteredConfig(),
            overrides=self._overrides(),
            butler=None,
        )
        # When config passed in, overrides ignored
        self.constructor.assert_called_with(config=self._alteredConfig(), initInputs=None, name=None)

    def testConfigButler(self):
        butler = self._tempButler()
        self.factory.makeTask(
            taskClass=self.constructor,
            label=None,
            config=self._alteredConfig(),
            overrides=None,
            butler=butler,
        )
        catalog = butler.get("fakeInitInput")  # Copies of _dummyCatalog are identical but not equal
        self.constructor.assert_called_with(
            config=self._alteredConfig(), initInputs={"initInput": catalog}, name=None
        )

    def testOverridesButler(self):
        butler = self._tempButler()
        self.factory.makeTask(
            taskClass=self.constructor, label=None, config=None, overrides=self._overrides(), butler=butler
        )
        config = FakeConfig()
        self._overrides().applyTo(config)
        catalog = butler.get("fakeInitInput")  # Copies of _dummyCatalog are identical but not equal
        self.constructor.assert_called_with(config=config, initInputs={"initInput": catalog}, name=None)
