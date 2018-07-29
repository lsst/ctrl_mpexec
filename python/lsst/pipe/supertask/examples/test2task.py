"""Simple example PipelineTask for testing purposes.
"""

import lsst.log
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = lsst.log.Log.getLogger(__name__)


class Test2Config(PipelineTaskConfig):
    input = InputDatasetField(name="input",
                              units=["Camera", "Visit"],
                              storageClass="example",
                              doc="Input dataset type for this task")
    output = OutputDatasetField(name="output",
                                units=["Tract", "Patch"],
                                storageClass="example",
                                doc="Output dataset type for this task")

    def setDefaults(self):
        # this task combines all selected visits into a tract/patch, on
        # input it expects per-visit data, on output it produces per-patch.
        # Combining visits "destroys" Visit unit in a quantum.
        self.quantum.units = ["Camera", "Tract", "Patch"]


class Test2Task(PipelineTask):
    """Simple example PipelineTask.

    It reads input data that is expected to be a list of number, combines
    them into one and stores in output dataset.
    """
    ConfigClass = Test2Config
    _DefaultName = 'Test2'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, input, output):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        input : `list`
            Lsit of input data objects
        output : `list`
            List of units for output, not used in this simple task.

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing %s: input=%s output=%s", self.getName(), input, output)

        # for output data the order of items should correspond to the order
        # of units in `output` parameter, but in this simple case we expect
        # just one DataRef in both input and output.
        data = [sum(input)]

        # attribute name of struct is the same as a config field name
        return Struct(output=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
