"""Simple example PipelineTask for testing purposes.
"""

import lsst.log
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = lsst.log.Log.getLogger(__name__)


class PatchSkyMapTaskConfig(PipelineTaskConfig):
    coadd = InputDatasetField(name="deepCoadd_calexp",
                              units=["SkyMap", "Tract", "Patch", "AbstractFilter"],
                              storageClass="Exposure",
                              doc="DatasetType for the input image")
    inputCatalog = InputDatasetField(name="deepCoadd_mergeDet",
                                     units=["SkyMap", "Tract", "Patch"],
                                     storageClass="SourceCatalog",
                                     doc="DatasetType for the input catalog (merged detections).")
    outputCatalog = OutputDatasetField(name="deepCoadd_meas",
                                       units=["SkyMap", "Tract", "Patch", "AbstractFilter"],
                                       storageClass="SourceCatalog",
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

    def run(self, coadd, inputCatalog, outputCatalog):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        coadd : `list`
            List of input data objects
        inputCatalog : `list`
            List of input data objects
        outputCatalog : `list`
            List of units for output, not used in this simple task.

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing %s: coadd=%s inputCatalog=%s outputCatalog=%s",
                  self.getName(), coadd, inputCatalog, outputCatalog)

        # output data, length must be equal to len(outputCatalog)
        data = [None]

        # attribute name of struct is the same as a config field name
        return Struct(outputCatalog=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
