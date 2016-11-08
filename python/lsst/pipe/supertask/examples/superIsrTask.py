from __future__ import division, absolute_import
#
# LSST Data Management System
#
# Copyright 2016  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
import lsst.pipe.base as pipeBase
from lsst.pipe.supertask import SuperTask
from lsst.ip.isr import IsrTask, IsrTaskConfig


class SuperIsrConfig(IsrTaskConfig):
    pass

class SuperIsrTask(SuperTask):
    """!A SuperTask version of IsrTask"""
    ConfigClass = SuperIsrConfig
    _DefaultName = "isr"

    def __init__(self, *args, **kwargs):
        super(SuperIsrTask, self).__init__(*args, **kwargs)
        self.makeSubtask("assembleCcd")
        self.makeSubtask("fringe")

    def read_input_data(self, dataRef):
        """Read needed data thru Butler in a pipeBase.Struct based on config"""
        ccdExp = dataRef.get('raw', immediate=True)
        isrData = IsrTask.readIsrData(IsrTask(self.config), dataRef, ccdExp)
        isrDataDict = isrData.getDict()
        return pipeBase.Struct(ccdExposure=ccdExp,
                               bias=isrDataDict['bias'],
                               linearizer=isrDataDict['linearizer'],
                               dark=isrDataDict['dark'],
                               flat=isrDataDict['flat'],
                               defects=isrDataDict['defects'],
                               fringes=isrDataDict['fringes'],
                               bfKernel=isrDataDict['bfKernel'])

    def write_output_data(self, dataRef, result):
        """Write output data thru Butler based on config"""
        if self.config.doWrite:
            dataRef.put(result.exposure, "postISRCCD")

    @pipeBase.timeMethod
    def execute(self, dataRef):
        """!Apply common instrument signature correction algorithms to a raw frame

        @param dataRef: butler data reference
        @return a pipeBase Struct containing:
        - exposure

        similar to IsrTask.runDataRef()
        """
        self.log.info("Performing Super ISR on sensor data ID %s" % (dataRef.dataId,))

        # IsrTask.runDataRef includes these three steps
        self.log.info("Reading input data using dataRef")
        inputData = self.read_input_data(dataRef)

        self.log.info("Running operations. The run() method should not take anything Butler")
        result = IsrTask.run(IsrTask(self.config), **inputData.getDict())

        self.log.info("Writing output data using dataRef")
        self.write_output_data(dataRef, result)

        return result

    @classmethod
    def _makeArgumentParser(cls):
        """!Create and return an argument parser

        @param[in] cls      the class object
        @return the argument parser for this task.

        This override is used to delay making the data ref list until the daset type is known;
        this is done in @ref parseAndRun.
        """
        parser = pipeBase.ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument(name="--id",
                               datasetType=pipeBase.ConfigDatasetType(name="datasetType"),
                               help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser
