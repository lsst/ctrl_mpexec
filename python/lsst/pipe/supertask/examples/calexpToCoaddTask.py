"""Simple example SuperTask for testing purposes.
"""

import lsst.log as lsstLog
from lsst.pex.config import ConfigField
from lsst.pipe import supertask
from lsst.pipe.base.struct import Struct

_LOG = lsstLog.Log.getLogger(__name__)


class CalexpToCoaddTaskConfig(supertask.SuperTaskConfig):
    calexp = ConfigField(dtype=supertask.InputDatasetConfig,
                         doc="DatasetType for the input image")
    coadd = ConfigField(dtype=supertask.OutputDatasetConfig,
                        doc="DatasetType for the output image")

    def setDefaults(self):
        # set units of a quantum, this task uses per-tract-patch-filter quanta
        self.quantum.units = ["SkyMap", "Tract", "Patch", "AbstractFilter"]
        self.quantum.sql = None

        self.calexp.name = "calexp"
        self.calexp.units = ["Camera", "Visit", "Sensor"]
        self.calexp.storageClass = "ExposureF"

        self.coadd.name = "deepCoadd_calexp"
        self.coadd.units = ["SkyMap", "Tract", "Patch", "AbstractFilter"]
        self.coadd.storageClass = "ExposureF"


class CalexpToCoaddTask(supertask.SuperTask):
    """Simple example SuperTask.
    """
    ConfigClass = CalexpToCoaddTaskConfig
    _DefaultName = 'calexpToCoaddTask'

    def run(self, calexp, coadd):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        calexp : `list`
            List of input data objects
        coadd : `list`
            List of units for output.

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing supertask: calexp=%s coadd=%s",
                  calexp, coadd)

        # output data, length must be equal to len(outputCatalog)
        data = [None]

        # attribute name of struct is the same as a config field name
        return Struct(coadd=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
