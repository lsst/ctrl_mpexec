"""Simple example SuperTask for testing purposes.
"""

from __future__ import absolute_import, division, print_function

import lsst.log as lsstLog
import lsst.pex.config as pexConfig
from lsst.pipe  import supertask
from lsst.pipe.base.struct import Struct

_LOG = lsstLog.Log.getLogger(__name__)


class Test2Config(supertask.SuperTaskConfig):
    input = pexConfig.ConfigField(dtype=supertask.InputDatasetConfig,
                                  doc="Input dataset type for this task")
    output = pexConfig.ConfigField(dtype=supertask.OutputDatasetConfig,
                                   doc="Output dataset type for this task")

    def setDefaults(self):
        # this task combines all selected visits into a tract/patch, on
        # input it expects per-visit data, on output it produces per-patch.
        # Combining visits "destroys" Visit unit in a quantum.
        self.quantum.units = ["Camera", "Tract", "Patch"]
        self.quantum.sql = None

        # default config for input dataset type
        self.input.name = "input"
        self.input.units = ["Camera", "Visit"]
        self.input.storageClass = "example"

        # default config for output dataset type
        self.output.name = "output"
        self.output.units = ["Tract", "Patch"]
        self.output.storageClass = "example"


class Test2Task(supertask.SuperTask):
    """Simple example SuperTask.

    It reads input data that is expected to be a list of number, combines
    them into one and stores in output dataset.
    """
    ConfigClass = Test2Config
    _DefaultName = 'Test2'

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
        data = [sum(data)]

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
