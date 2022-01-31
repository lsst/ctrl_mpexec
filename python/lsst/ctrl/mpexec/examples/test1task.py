"""Simple example PipelineTask for testing purposes.

There is no reasonable way to execute this task but it can be used for
building Pipeline or QuantumGraph.
"""

import logging

from lsst.pipe.base import PipelineTask, PipelineTaskConfig, PipelineTaskConnections, Struct
from lsst.pipe.base import connectionTypes as cT

_LOG = logging.getLogger(__name__)


class Test1Connections(PipelineTaskConnections, dimensions=("instrument", "visit")):
    input = cT.Input(
        name="input",
        dimensions=["instrument", "visit"],
        storageClass="example",
        doc="Input dataset type for this task",
    )
    output = cT.Output(
        name="output",
        dimensions=["instrument", "visit"],
        storageClass="example",
        doc="Output dataset type for this task",
    )


class Test1Config(PipelineTaskConfig, pipelineConnections=Test1Connections):
    pass


class Test1Task(PipelineTask):
    """Simple example PipelineTask.

    It reads input data that is expected to be a number, performs
    simple arithmetic on that and stores in output dataset.
    """

    ConfigClass = Test1Config
    _DefaultName = "Test1"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, input):
        """Operate on in-memory data.

        With default implementation of `runQuantum()` keyword arguments
        correspond to field names in a config.

        Parameters
        ----------
        input : object
            Input data objects (scalar)

        Returns
        -------
        `Struct` instance with produced result.
        """

        _LOG.info("executing %s: input=%s", self.getName(), input)

        data = input**2

        # attribute name of struct is the same as a config field name
        return Struct(output=data)

    def __str__(self):
        return "{}(name={})".format(self.__class__.__name__, self.getName())
