"""
Module defining ExampleStorageClass class and related methods.
"""

from __future__ import absolute_import, division, print_function

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import StorageClass
from lsst.pipe.supertask import SuperTask

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class ExampleStorageClass(StorageClass):
    """Implementation of the StorageClass used in examples in this package.
    """
    pass


# This registers ExampleStorageClass with the "global" SuperTask
# StorageClass factory. This is useful for unit tests to avoid
# multiple registration from each unit test.
SuperTask.storageClassFactory.registerStorageClass(ExampleStorageClass("example"))
