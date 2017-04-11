"""Example SuperTask to demonstrate use of quanta and DIG.

The SuperTask in this module is not a working something but rather a code
example of how implementation of certain methods could look like with the
SuperTask quanta and DataIdGenerartors. This is loosely based on
CharacterizeImageTask as one of the simple tasks w.r.t. input/output
datasets (ignoring real life complexities).
"""

import lsst.pex.config as pexConfig
from lsst.pipe.digs import (ObsDateDIG, CcdDIG, CcdDIGConfig,
                            BackgroundDIG, BackgroundDIGConfig,
                            ExposureIdDIG, ExposureIdDIGConfig)
from lsst.pipe.supertask import Quantum, SuperTask

class CharacterizeImageConfig(pexConfig.Config):
    """In this config class we define only fields that are relevant to
    this example, the rest should be "copied" without change from
    pipe.tasks.CharacterizeImageConfig.
    """

    obsDateDig = pexConfig.ConfigurableField(
        target=ObsDateDIG,
        doc="DataIdGenerator for observation data-based visit selection",
    )
    ccdDig = pexConfig.ConfigurableField(
        target=CcdDIG,
        doc="DataIdGenerator for per-CCD splitting",
        default=CcdDIGConfig(dataset="postISRCCD")
    )
    bckgDig = pexConfig.ConfigurableField(
        target=BackgroundDIG,
        doc="DataIdGenerator for background images",
        default=BackgroundDIGConfig(ccdDataset="postISRCCD",
                                    dataset="background")
    )
    expIdDig = pexConfig.ConfigurableField(
        target=ExposureIdDIG,
        doc="DataIdGenerator for exposure ID",
        default=ExposureIdDIGConfig(dataset="expIdInfo")
    )
    # ... a lot more fields

class CharacterizeImageTask(SuperTask):
    """Image characterization SuperTask

    This is just a code example, do not expect it to do anything useful.
    """

    ConfigClass = CharacterizeImageConfig
    _DefaultName = "characterizeImage"

    def __init__(self, config=None, butler=None):
        SuperTask.__init__(self, config=config, butler=butler)

        # instantiate DIGs
        self._obsDateDig = self.makeDig("obsDateDig")
        self._ccdDig = self.makeDig("ccdDig")
        self._bckgDig = self.makeDig("bckgDig")
        self._expIdDig = self.makeDig("expIdDig")

        # define all sub-tasks

    def define_quanta(self, inputs, outputs, butler):
        """
        Parameters
        ----------
        inputs : `dict` of '{str: list}`
            Maps dataset type name into list of corresponding DataIds.
        outputs : `dict` of '{str: list}`
            Maps dataset type name into list of corresponding DataIds.
        butler : object
            Data butler instance, can be used for metadata-only lookups.

        Returns
        -------
        List of `Quantum` instances.
        """

        quanta = []

        # expect outputs to be empty, ignore it for now

        # in case inputs were given as observation date
        inputs = self._obsDateDig.computeIds(inputs, [])

        # split into individual CCD-level dataIds
        inputs = self._ccdDig.computeIds(inputs, [])

        # loop over all CCD and get matching other dataIds,
        # this is very inefficient, maybe we can have different
        # DIG API which can return matched dataIds
        for ccdDataId in inputs:

            # input data
            bkgDataIds = self._bckgDig.computeIds([ccdDataId], [])
            expIdInfoIds = self._expIdDig.computeIds([ccdDataId], [])

            # this assumes that output dataIds match 1-to-1 ccdDataId,
            # but in general we'll need separate DIG to map those too
            icSrcDataId = [ccdDataId]
            icExpDataId = [ccdDataId]
            icBckgDataId = [ccdDataId]

            # map dataset types to dataIds, names should match the names
            # that are used by DIGs, maybe we should get names from DIGs directly
            ccdInputs = dict(postISRCCD=ccdDataId,
                             background=bkgDataIds,
                             expIdInfo=expIdInfoIds)
            ccdOutputs = dict(icSrc=icSrcDataId,
                              icExp=icExpDataId,
                              icExpBackground=icBckgDataId)
            quanta.append(Quantum(ccdInputs, ccdOutputs))

        return quanta

    def run_quantum(self, quantum, butler):
        """Execute SuperTask algorithm on single quantum of data.

        Parameters
        ----------
        quantum : `Quantum`
            Object describing input and output corresponding to this
            invocation of SuperTask instance.
        butler : object
            Data butler instance.
        """

        # Currently run() method looks like
        #     run(self, exposure, exposureIdInfo=None, background=None)
        # and we need to manually map inputs to arguments.
        # We could make this method more generic if run() has arguments named
        # after dataset types each taking a list of objects.

        # no error checking for now and assuming that each dataId list has 1 element
        inputs = quantum.inputs
        exposure = butler.get("postISRCCD", inputs["postISRCCD"][0])
        background = butler.get("background", inputs["background"][0])
        exposureIdInfo = butler.get("expIdInfo", inputs["expIdInfo"][0])

        # it's run() even though it is called
        charRes = self.run(exposure, exposureIdInfo, background)

        # unpack and store in butler, lots of assumptions here
        outputs = quantum.outputs
        butler.put(charRes.sourceCat, "icSrc", outputs["icSrc"][0])
        butler.put(charRes.exposure, "icExp", outputs["icExp"][0])
        butler.put(charRes.background, "icExpBackground", outputs["icExpBackground"][0])

    def run(self, exposure, exposureIdInfo=None, background=None):
        """Characterize a science image
        """
        pass
