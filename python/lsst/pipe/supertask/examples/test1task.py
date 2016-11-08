"""
Test1 Task
"""
from __future__ import absolute_import, division, print_function
from builtins import str
from lsst.pipe.supertask.super_task import SuperTask
from lsst.pipe.base.struct import Struct
import lsst.pex.config as pexConfig


class Test1Config(pexConfig.Config):

    """
    Config
    """
    do_print = pexConfig.Field(
        dtype=bool,
        doc="Display info",
        default=False,
    )


class Test1Task(SuperTask):

    """
    Task
    """
    ConfigClass = Test1Config  # ConfigClass = pexConfig.Config
    _DefaultName = 'Test1'

    def __init__(self, *args, **kwargs):
        super(Test1Task, self).__init__(*args, **kwargs)  # # P3 would be super().__init__()

    def execute(self, dataRef):
        return self.run()

    def run(self):
        """
        Run method
        :return:
        """
        print('I am running %s' % (self.name,))
        if self.config.do_print:
            print("Displaying Info...")

        self.output = Struct(
            val1=10.,
            str1='test')

        return self.output

    def _get_config_name(self):
        """!Get the name prefix for the task config's dataset type, or None to prevent persisting the config

        This override returns None to avoid persisting metadata for this trivial task.
        """
        return None

    def __str__(self):
        return str(self.__class__.__name__)+' named : '+self.name
