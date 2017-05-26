"""
Test2 Task
"""
from __future__ import absolute_import, division, print_function
from builtins import str
from lsst.pipe.supertask import SuperTask
from lsst.pipe.base.struct import Struct
import lsst.pex.config as pexConfig


class Test2Config(pexConfig.Config):

    """
    Config
    """
    maxval = pexConfig.Field(
        dtype=int,
        doc="Max value",
        default=22,
    )


class Test2Task(SuperTask):

    """
    Task
    """
    ConfigClass = Test2Config  # ConfigClass = pexConfig.Config
    _DefaultName = 'test2'

    def __init__(self, *args, **kwargs):
        super(Test2Task, self).__init__(*args, **kwargs)  # # P3 would be super().__init__()

    def execute(self, dataRef):
        return self.run()

    def run(self):
        """
        Run method
        :return:
        """
        print('I am running %s' % (self.getName(),))

        myvalue = 2.5

        self.output = Struct(
            val2=myvalue,
            str2='value 2')
        return self.output

    def _get_config_name(self):
        """!Get the name prefix for the task config's dataset type, or None to prevent persisting the config

        This override returns None to avoid persisting metadata for this trivial task.
        """
        return None

    def __str__(self):
        return str(self.__class__.__name__) + ' named : ' + self.getName()
