"""Simple example PipelineTask for testing purposes.
"""

import logging

from lsst.afw.image import ExposureF
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = logging.getLogger(__name__.partition(".")[2])


class RawToCalexpTaskConfig(PipelineTaskConfig):
    input = InputDatasetField(name="raw",
                              dimensions=["instrument", "exposure", "detector"],
                              storageClass="ExposureU",
                              doc="Input dataset type for this task")
    output = OutputDatasetField(name="calexp",
                                dimensions=["instrument", "visit", "detector"],
                                storageClass="ExposureF",
                                scalar=True,
                                doc="Output dataset type for this task")

    def setDefaults(self):
        # set dimensions of a quantum, this task uses per-visit-detector quanta
        self.quantum.dimensions = ["instrument", "visit", "detector"]


class RawToCalexpTask(PipelineTask):
    """Simple example PipelineTask.
    """
    ConfigClass = RawToCalexpTaskConfig
    _DefaultName = 'RawToCalexpTask'

    def run(self, input):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        input : `list`
            List of input data objects

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing %s: input=%s", self.getName(), input)

        # result, scalar in this case, just create 100x100 image
        data = ExposureF(100, 100)

        # attribute name of struct is the same as a config field name
        return Struct(output=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
