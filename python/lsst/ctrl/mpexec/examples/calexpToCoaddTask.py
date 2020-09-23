"""Simple example PipelineTask for testing purposes.
"""

import logging

from lsst.afw.image import ExposureF
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            PipelineTaskConnections)
from lsst.pipe.base import connectionTypes as cT
import lsstDebug

_LOG = logging.getLogger(__name__.partition(".")[2])


class CalexpToCoaddTaskConnections(PipelineTaskConnections,
                                   dimensions=("skymap", "tract", "patch", "band")):
    calexp = cT.Input(name="calexp",
                      dimensions=["instrument", "visit", "detector"],
                      multiple=True,
                      storageClass="ExposureF",
                      doc="DatasetType for the input image")
    coadd = cT.Output(name="deepCoadd_calexp",
                      dimensions=["skymap", "tract", "patch", "band"],
                      storageClass="ExposureF",
                      doc="DatasetType for the output image")


class CalexpToCoaddTaskConfig(PipelineTaskConfig, pipelineConnections=CalexpToCoaddTaskConnections):
    pass


class CalexpToCoaddTask(PipelineTask):
    """Simple example PipelineTask.
    """
    ConfigClass = CalexpToCoaddTaskConfig
    _DefaultName = 'calexpToCoaddTask'

    def run(self, calexp):
        """Operate on in-memory data.

        Returns
        -------
        `Struct` instance with produced result.
        """
        _LOG.info("executing %s: calexp=%s", self.getName(), calexp)

        # To test lsstDebug function make a debug.py file with this contents
        # somewhere in PYTHONPATH and run `pipetask` with --debug option:
        #
        #    import lsstDebug
        #    lsstDebug.Info('lsst.ctrl.mpexec.examples.calexpToCoaddTask').display = True
        #
        if lsstDebug.Info(__name__).display:
            _LOG.info("%s: display enabled", __name__)

        # output data, scalar in this case
        data = ExposureF(100, 100)

        # attribute name of struct is the same as a config field name
        return Struct(coadd=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
