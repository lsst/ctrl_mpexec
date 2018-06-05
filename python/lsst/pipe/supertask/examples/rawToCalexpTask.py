"""Simple example SuperTask for testing purposes.
"""

import lsst.log as lsstLog
import lsst.pex.config as pexConfig
from lsst.pipe import supertask
from lsst.pipe.base.struct import Struct

_LOG = lsstLog.Log.getLogger(__name__)


class RawToCalexpTaskConfig(supertask.SuperTaskConfig):
    input = pexConfig.ConfigField(dtype=supertask.InputDatasetConfig,
                                  doc="Input dataset type for this task")
    output = pexConfig.ConfigField(dtype=supertask.OutputDatasetConfig,
                                   doc="Output dataset type for this task")

    def setDefaults(self):
        # set units of a quantum, this task uses per-visit-sensor quanta
        self.quantum.units = ["Camera", "Visit", "Sensor"]
        self.quantum.sql = None

        # default config for input dataset type
        self.input.name = "raw"
        self.input.units = ["Camera", "Exposure", "Sensor"]
        self.input.storageClass = "example"

        # default config for output dataset type
        self.output.name = "calexp"
        self.output.units = ["Camera", "Visit", "Sensor"]
        self.output.storageClass = "example"


class RawToCalexpTask(supertask.SuperTask):
    """Simple example SuperTask.
    """
    ConfigClass = RawToCalexpTaskConfig
    _DefaultName = 'RawToCalexpTask'

    def run(self, input, output):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        input : `list`
            List of input data objects
        output : `list`
            List of units for output, not used in this simple task.

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing supertask: input=%s output=%s", input, output)

        data = input

        # attribute name of struct is the same as a config field name
        return Struct(output=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
