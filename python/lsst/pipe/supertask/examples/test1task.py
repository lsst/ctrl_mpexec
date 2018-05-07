"""Simple example SuperTask for testing purposes.
"""

from __future__ import absolute_import, division, print_function

import lsst.log as lsstLog
import lsst.pex.config as pexConfig
from lsst.pipe import supertask
from lsst.pipe.base.struct import Struct

_LOG = lsstLog.Log.getLogger(__name__)


class Test1Config(supertask.SuperTaskConfig):
    input = pexConfig.ConfigField(dtype=supertask.InputDatasetConfig,
                                  doc="Input dataset type for this task")
    output = pexConfig.ConfigField(dtype=supertask.OutputDatasetConfig,
                                   doc="Output dataset type for this task")

    def setDefaults(self):
        # set units of a quantum, this task uses per-visit quanta and it
        # expects datset units to be the same
        self.quantum.units = ["Camera", "Visit"]
        self.quantum.sql = None

        # default config for input dataset type
        self.input.name = "input"
        self.input.units = ["Camera", "Visit"]
        self.input.storageClass = "example"

        # default config for output dataset type
        self.output.name = "output"
        self.output.units = ["Camera", "Visit"]
        self.output.storageClass = "example"


class Test1Task(supertask.SuperTask):
    """Simple example SuperTask.

    It reads input data that is expected to be a number, performs
    simple arithmetic on that and stores in output dataset.
    """
    ConfigClass = Test1Config
    _DefaultName = 'Test1'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, input, output):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        input : `list`
            Lsit of input data objects
        output : `list`
            List of units for output, not used in this simple task.

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing supertask: input=%s output=%s", input, output)

        # for output data the order of items should correspond to the order
        # of units in `output` parameter, but in this simple case we expect
        # just one DataRef in both input and output.
        data = [x ** 2 for x in input]

        # attribute name of struct is the same as a config field name
        return Struct(output=data)

    def _get_config_name(self):
        """Get the name prefix for the task config's dataset type, or None
        to prevent persisting the config

        This override returns None to avoid persisting metadata for this
        trivial task.
        """
        return None

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
