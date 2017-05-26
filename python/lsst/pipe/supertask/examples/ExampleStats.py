from __future__ import absolute_import, division, print_function

import lsst.pex.config as pexConfig

from lsst.afw.image import MaskU
import lsst.afw.math as afwMath
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.pipe.supertask import SuperTask


class ExampleMeanConfig(pexConfig.Config):

    """!Configuration for ExampleSigmaClippedStatsTask
    """
    badMaskPlanes = pexConfig.ListField(
        dtype=str,
        doc="Mask planes that, if set, the associated pixel should not be included in the coaddTempExp.",
        default=("EDGE",),
    )
    numSigmaClip = pexConfig.Field(
        doc="number of sigmas at which to clip data",
        dtype=float,
        default=3.0,
    )
    numIter = pexConfig.Field(
        doc="number of iterations of sigma clipping",
        dtype=int,
        default=2,
    )


class ExampleStdConfig(pexConfig.Config):

    """!Configuration for ExampleSigmaClippedStatsTask
    """
    badMaskPlanes = pexConfig.ListField(
        dtype=str,
        doc="Mask planes that, if set, the associated pixel should not be included in the coaddTempExp.",
        default=("EDGE",),
    )
    numSigmaClip = pexConfig.Field(
        doc="number of sigmas at which to clip data",
        dtype=float,
        default=3.0,
    )
    numIter = pexConfig.Field(
        doc="number of iterations of sigma clipping",
        dtype=int,
        default=2,
    )


class ExampleMeanTask(SuperTask):

    ConfigClass = ExampleMeanConfig
    _DefaultName = "exampleMean"

    def __init__(self, *args, **kwargs):

        super(ExampleMeanTask, self).__init__(*args, **kwargs)
        #basetask.Task.__init__(self, *args, **kwargs)

    @pipeBase.timeMethod
    def execute(self, dataRef):
        self.log.info("Processing data ID %s" % (dataRef.dataId,))
        self._badPixelMask = MaskU.getPlaneBitMask(self.config.badMaskPlanes)
        self._statsControl = afwMath.StatisticsControl()
        self._statsControl.setNumSigmaClip(self.config.numSigmaClip)
        self._statsControl.setNumIter(self.config.numIter)
        self._statsControl.setAndMask(self._badPixelMask)

        rawExp = dataRef.get("raw")
        maskedImage = rawExp.getMaskedImage()
        return self.run(maskedImage)

    def run(self, maskedImage):

        statObj = afwMath.makeStatistics(maskedImage, afwMath.MEANCLIP | afwMath.STDEVCLIP | afwMath.ERRORS,
                                         self._statsControl)
        mean, meanErr = statObj.getResult(afwMath.MEANCLIP)
        self.log.info("clipped mean=%0.2f; meanErr=%0.2f" % (mean, meanErr))

        self.output = pipeBase.Struct(
            mean=mean,
            meanErr=meanErr,
        )
        return self.output

    @classmethod
    def makeArgumentParser(cls):
        parser = pipeBase.InputOnlyArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", "raw", help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser

    def _get_config_name(self):
        """!Get the name prefix for the task config's dataset type, or None to prevent persisting the config

        This override returns None to avoid persisting metadata for this trivial task.
        """
        return None


class ExampleStdTask(SuperTask):

    ConfigClass = ExampleStdConfig
    _DefaultName = "exampleStd"

    def __init__(self, *args, **kwargs):

        super(ExampleStdTask, self).__init__(*args, **kwargs)

    @pipeBase.timeMethod
    def execute(self, dataRef):
        self.log.info("Processing data ID %s" % (dataRef.dataId,))
        self._badPixelMask = MaskU.getPlaneBitMask(self.config.badMaskPlanes)
        self._statsControl = afwMath.StatisticsControl()
        self._statsControl.setNumSigmaClip(self.config.numSigmaClip)
        self._statsControl.setNumIter(self.config.numIter)
        self._statsControl.setAndMask(self._badPixelMask)
        rawExp = dataRef.get("raw")
        maskedImage = rawExp.getMaskedImage()
        return self.run(maskedImage)

    def run(self, maskedImage):
        statObj = afwMath.makeStatistics(maskedImage, afwMath.MEANCLIP | afwMath.STDEVCLIP | afwMath.ERRORS,
                                         self._statsControl)
        stdDev, stdDevErr = statObj.getResult(afwMath.STDEVCLIP)
        self.log.info("stdDev=%0.2f; stdDevErr=%0.2f" %
                      (stdDev, stdDevErr))
        self.output = pipeBase.Struct(
            stdDev=stdDev,
            stdDevErr=stdDevErr,
        )
        return self.output

    @classmethod
    def makeArgumentParser(cls):
        parser = pipeBase.InputOnlyArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", "raw", help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser

    def _get_config_name(self):
        """!Get the name prefix for the task config's dataset type, or None to prevent persisting the config

        This override returns None to avoid persisting metadata for this trivial task.
        """
        return None
