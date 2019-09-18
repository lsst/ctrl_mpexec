"""Simple example PipelineTask for testing purposes.
"""

import logging

from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            PipelineTaskConnections)
from lsst.pipe.base import connectionTypes as cT

_LOG = logging.getLogger(__name__.partition(".")[2])


class PatchSkyMapTaskConnections(PipelineTaskConnections,
                                 dimensions=("skymap", "tract", "patch", "abstract_filter")):
    coadd = cT.Input(name="deepCoadd_calexp",
                     dimensions=["skymap", "tract", "patch", "abstract_filter"],
                     storageClass="ExposureF",
                     doc="DatasetType for the input image")
    inputCatalog = cT.Input(name="deepCoadd_mergeDet",
                            dimensions=["skymap", "tract", "patch"],
                            storageClass="SourceCatalog",
                            doc="DatasetType for the input catalog (merged detections).")
    outputCatalog = cT.Output(name="deepCoadd_meas",
                              dimensions=["skymap", "tract", "patch", "abstract_filter"],
                              storageClass="SourceCatalog",
                              doc=("DatasetType for the output catalog "
                                   "(deblended per-band measurements)"))


class PatchSkyMapTaskConfig(PipelineTaskConfig,
                            pipelineConnections=PatchSkyMapTaskConnections):
    pass


class PatchSkyMapTask(PipelineTask):
    """Simple example PipelineTask.
    """
    ConfigClass = PatchSkyMapTaskConfig
    _DefaultName = 'patchSkyMapTask'

    def run(self, coadd, inputCatalog):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        coadd : object
            Input data object (input dataset type is configured as scalar)
        inputCatalog : object
            Input data object (input dataset type is configured as scalar)

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing %s: coadd=%s inputCatalog=%s",
                  self.getName(), coadd, type(inputCatalog))

        # output data, scalar in this case, just return input catalog without change
        data = inputCatalog

        # attribute name of struct is the same as a config field name
        return Struct(outputCatalog=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
