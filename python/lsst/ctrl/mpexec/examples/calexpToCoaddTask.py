"""Simple example PipelineTask for testing purposes.
"""

import logging

from lsst.afw.image import ExposureF
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = logging.getLogger(__name__.partition(".")[2])


class CalexpToCoaddTaskConfig(PipelineTaskConfig):
    calexp = InputDatasetField(name="calexp",
                               dimensions=["Instrument", "Visit", "Detector"],
                               storageClass="ExposureF",
                               doc="DatasetType for the input image")
    coadd = OutputDatasetField(name="deepCoadd_calexp",
                               dimensions=["SkyMap", "Tract", "Patch", "AbstractFilter"],
                               storageClass="ExposureF",
                               scalar=True,
                               doc="DatasetType for the output image")

    def setDefaults(self):
        # set dimensions of a quantum, this task uses per-tract-patch-filter quanta
        self.quantum.dimensions = ["SkyMap", "Tract", "Patch", "AbstractFilter"]


class CalexpToCoaddTask(PipelineTask):
    """Simple example PipelineTask.
    """
    ConfigClass = CalexpToCoaddTaskConfig
    _DefaultName = 'calexpToCoaddTask'

    def adaptArgsAndRun(self, inputData, inputDataIds, outputDataIds, butler):
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
