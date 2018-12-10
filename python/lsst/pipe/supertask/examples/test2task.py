"""Simple example PipelineTask for testing purposes.

There is no reasonable way to execute this task but it can be used for
building Pipeline or QuantumGraph.
"""

import logging

from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField)

_LOG = logging.getLogger(__name__.partition(".")[2])


class Test2Config(PipelineTaskConfig):
    input = InputDatasetField(name="input",
                              dimensions=["Instrument", "Visit"],
                              storageClass="example",
                              doc="Input dataset type for this task")
    output = OutputDatasetField(name="output",
                                dimensions=["Tract", "Patch"],
                                storageClass="example",
                                scalar=True,
                                doc="Output dataset type for this task")

    def setDefaults(self):
        # this task combines all selected visits into a tract/patch, on
        # input it expects per-visit data, on output it produces per-patch.
        # Combining visits "destroys" Visit dimension in a quantum.
        self.quantum.dimensions = ["Instrument", "Tract", "Patch"]


class Test2Task(PipelineTask):
    """Simple example PipelineTask.

    It reads input data that is expected to be a list of number, combines
    them into one and stores in output dataset.
    """
    ConfigClass = Test2Config
    _DefaultName = 'Test2'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, input):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        input : `list`
            List of input data objects

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing %s: input=%s", self.getName(), input)

        # result, scalar
        data = sum(input)

        # attribute name of struct is the same as a config field name
        return Struct(output=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
