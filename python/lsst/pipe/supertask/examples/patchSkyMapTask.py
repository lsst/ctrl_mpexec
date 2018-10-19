"""Simple example PipelineTask for testing purposes.
"""

import logging

from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = logging.getLogger(__name__.partition(".")[2])


class PatchSkyMapTaskConfig(PipelineTaskConfig):
    coadd = InputDatasetField(name="deepCoadd_calexp",
                              units=["SkyMap", "Tract", "Patch", "AbstractFilter"],
                              storageClass="ExposureF",
                              scalar=True,
                              doc="DatasetType for the input image")
    inputCatalog = InputDatasetField(name="deepCoadd_mergeDet",
                                     units=["SkyMap", "Tract", "Patch"],
                                     storageClass="SourceCatalog",
                                     scalar=True,
                                     doc="DatasetType for the input catalog (merged detections).")
    outputCatalog = OutputDatasetField(name="deepCoadd_meas",
                                       units=["SkyMap", "Tract", "Patch", "AbstractFilter"],
                                       storageClass="SourceCatalog",
                                       scalar=True,
                                       doc=("DatasetType for the output catalog "
                                            "(deblended per-band measurements)"))

    def setDefaults(self):
        # set units of a quantum, this task uses per-tract-patch-filter quanta
        self.quantum.units = ["SkyMap", "Tract", "Patch", "AbstractFilter"]


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
