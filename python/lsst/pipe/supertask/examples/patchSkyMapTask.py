"""Simple example SuperTask for testing purposes.
"""

import lsst.log as lsstLog
from lsst.pex.config import ConfigField
from lsst.pipe import supertask
from lsst.pipe.base.struct import Struct

_LOG = lsstLog.Log.getLogger(__name__)


class PatchSkyMapTaskConfig(supertask.SuperTaskConfig):
    coadd = ConfigField(dtype=supertask.InputDatasetConfig,
                        doc="DatasetType for the input image")
    inputCatalog = ConfigField(dtype=supertask.InputDatasetConfig,
                               doc="DatasetType for the input catalog (merged detections).")
    outputCatalog = ConfigField(dtype=supertask.OutputDatasetConfig,
                                doc="DatasetType for the output catalog (deblended per-band measurements)")

    def setDefaults(self):
        # set units of a quantum, this task uses per-tract-patch-filter quanta
        self.quantum.units = ["SkyMap", "Tract", "Patch", "AbstractFilter"]
        self.quantum.sql = None

        self.coadd.name = "deepCoadd_calexp"
        self.coadd.units = ["SkyMap", "Tract", "Patch", "AbstractFilter"]
        self.coadd.storageClass = "Exposure"

        self.inputCatalog.name = "deepCoadd_mergeDet"
        self.inputCatalog.units = ["SkyMap", "Tract", "Patch"]
        self.inputCatalog.storageClass = "SourceCatalog"

        self.outputCatalog.name = "deepCoadd_meas"
        self.outputCatalog.units = ["SkyMap", "Tract", "Patch", "AbstractFilter"]
        self.outputCatalog.storageClass = "SourceCatalog"


class PatchSkyMapTask(supertask.SuperTask):
    """Simple example SuperTask.
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

        _LOG.info("executing supertask: coadd=%s inputCatalog=%s outputCatalog=%s",
                  coadd, inputCatalog, outputCatalog)

        # output data, length must be equal to len(outputCatalog)
        data = [None]

        # attribute name of struct is the same as a config field name
        return Struct(outputCatalog=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
