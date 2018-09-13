"""Simple example PipelineTask for testing purposes.
"""

import lsst.log
from lsst.afw.image import ExposureF
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = lsst.log.Log.getLogger(__name__)


class CalexpToCoaddTaskConfig(PipelineTaskConfig):
    calexp = InputDatasetField(name="calexp",
                               units=["Camera", "Visit", "Sensor"],
                               storageClass="ExposureF",
                               doc="DatasetType for the input image")
    coadd = OutputDatasetField(name="deepCoadd_calexp",
                               units=["SkyMap", "Tract", "Patch", "AbstractFilter"],
                               storageClass="ExposureF",
                               scalar=True,
                               doc="DatasetType for the output image")

    def setDefaults(self):
        # set units of a quantum, this task uses per-tract-patch-filter quanta
        self.quantum.units = ["SkyMap", "Tract", "Patch", "AbstractFilter"]


class CalexpToCoaddTask(PipelineTask):
    """Simple example PipelineTask.
    """
    ConfigClass = CalexpToCoaddTaskConfig
    _DefaultName = 'calexpToCoaddTask'

    def adaptArgsAndRun(self, inputData, inputDataIds, outputDataIds):
        """Operate on in-memory data.

        Returns
        -------
        `Struct` instance with produced result.
        """
        # calexps = inputData["calexp"]
        calexpDataIds = inputDataIds["calexp"]
        coaddDataIds = outputDataIds["coadd"]
        _LOG.info("executing %s: calexp=%s coadd=%s",
                  self.getName(), calexpDataIds, coaddDataIds)

        # output data, scalar in this case
        data = ExposureF(100, 100)

        # attribute name of struct is the same as a config field name
        return Struct(coadd=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
