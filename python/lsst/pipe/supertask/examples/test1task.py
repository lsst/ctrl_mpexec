"""Simple example PipelineTask for testing purposes.
"""

import lsst.log
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = lsst.log.Log.getLogger(__name__)


class Test1Config(PipelineTaskConfig):
    input = InputDatasetField(name="input",
                              units=["Camera", "Visit"],
                              storageClass = "example",
                              doc="Input dataset type for this task")
    output = OutputDatasetField(name="output",
                                units=["Camera", "Visit"],
                                storageClass="example",
                                doc="Output dataset type for this task")

    def setDefaults(self):
        # set units of a quantum, this task uses per-visit quanta and it
        # expects datset units to be the same
        self.quantum.units = ["Camera", "Visit"]


class Test1Task(PipelineTask):
    """Simple example PipelineTask.

    It reads input data that is expected to be a number, performs
    simple arithmetic on that and stores in output dataset.
    """
    ConfigClass = Test1Config
    _DefaultName = 'Test1'

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
        data = [x ** 2 for x in input]

        # attribute name of struct is the same as a config field name
        return Struct(output=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
