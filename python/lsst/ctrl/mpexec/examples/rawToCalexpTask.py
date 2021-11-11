"""Simple example PipelineTask for testing purposes.
"""

import logging

from lsst.afw.image import ExposureF
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            PipelineTaskConnections)
from lsst.pipe.base import connectionTypes as cT

_LOG = logging.getLogger(__name__)


class RawToCalexpTaskConnections(PipelineTaskConnections,
                                 dimensions=("instrument", "visit", "detector")):
    input = cT.Input(name="raw",
                     dimensions=["instrument", "exposure", "detector"],
                     multiple=True,
                     storageClass="Exposure",
                     doc="Input dataset type for this task")
    output = cT.Output(name="calexp",
                       dimensions=["instrument", "visit", "detector"],
                       storageClass="ExposureF",
                       doc="Output dataset type for this task")


class RawToCalexpTaskConfig(PipelineTaskConfig,
                            pipelineConnections=RawToCalexpTaskConnections):
    pass


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
