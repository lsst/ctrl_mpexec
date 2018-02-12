"""Simple unit test for parser module.
"""
#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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
# see <http://www.lsstcorp.org/LegalNotices/>.
#

from argparse import ArgumentParser
from collections import OrderedDict
import unittest

import lsst.utils.tests
import lsst.pipe.supertask.parser as parser_mod


class _Error(Exception):
    pass

class _NoExitParser(ArgumentParser):
    """Special parser subclass which does not exit on errors or help.
    """

    def exit(self, status=0, message=None):
        pass

    def error(self, message):
        raise _Error(message)


class CmdLineParserTestCase(unittest.TestCase):
    """A test case for parser module
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testPipelineAction(self):
        """Test for a _PipelineAction and _pipe_action
        """

        parser = _NoExitParser()
        parser.add_argument("-t", dest="pipeline_actions", action='append',
                            type=parser_mod._ACTION_ADD_TASK)
        parser.add_argument("-m", dest="pipeline_actions", action='append',
                            type=parser_mod._ACTION_MOVE_TASK)

        PipelineAction = parser_mod._PipelineAction
        args = parser.parse_args("-t task".split())
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", None, "task")])

        args = parser.parse_args("-t task:label -m label:1".split())
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", "label", "task"),
                                                 PipelineAction("move_task", "label", "1")])

        with self.assertRaises(_Error):
            parser.parse_args("-m label".split())

        with self.assertRaises(_Error):
            parser.parse_args("-m label:notANumber".split())

    def testIdValueAction(self):
        """Test for parser._IdValueAction
        """
        parser = _NoExitParser()
        parser.add_argument("--type", dest='_data_type')
        parser.add_argument("--id", dest='id', nargs="+",
                            action=parser_mod._IdValueAction)

        args = parser.parse_args([])
        self.assertTrue(hasattr(args, 'id'))

#         with self.assertRaises(_Error):
#             args = parser.parse_args("--id key=1".split())

        args = parser.parse_args("--type type1 --id key=1".split())
        self.assertTrue(isinstance(args.id, dict))
        expect = dict(type1=[OrderedDict([("key", "1")])])
        self.assertEqual(args.id, expect)

        args = parser.parse_args("--type type1 --id key=1 foo=bar".split())
        expect = dict(type1=[OrderedDict([("key", "1"), ("foo", "bar")])])
        self.assertEqual(args.id, expect)

        args = parser.parse_args("--type type1 --id key=1^3 foo=bar^baz".split())
        expect = dict(type1=[OrderedDict([("key", "1"), ("foo", "bar")]),
                             OrderedDict([("key", "1"), ("foo", "baz")]),
                             OrderedDict([("key", "3"), ("foo", "bar")]),
                             OrderedDict([("key", "3"), ("foo", "baz")])])
        self.assertEqual(args.id, expect)

        args = parser.parse_args("--type type1 --id key=1^3..5 foo=bar".split())
        expect = dict(type1=[OrderedDict([("key", "1"), ("foo", "bar")]),
                             OrderedDict([("key", "3"), ("foo", "bar")]),
                             OrderedDict([("key", "4"), ("foo", "bar")]),
                             OrderedDict([("key", "5"), ("foo", "bar")])])
        self.assertEqual(args.id, expect)

        with self.assertRaises(_Error):
            parser.parse_args("--type x --id key=1 key=2".split())

        args = parser.parse_args("--type type1 --id key=1 foo=bar --id key=2 foo=baz".split())
        expect = dict(type1=[OrderedDict([("key", "1"), ("foo", "bar")]),
                             OrderedDict([("key", "2"), ("foo", "baz")])])
        self.assertEqual(args.id, expect)

        args = parser.parse_args("--type type1 --id key=1 foo=bar --type type2 --id key=2 foo=baz".split())
        expect = dict(type1=[OrderedDict([("key", "1"), ("foo", "bar")])],
                      type2=[OrderedDict([("key", "2"), ("foo", "baz")])])
        self.assertEqual(args.id, expect)

    def testCmdLineParser(self):
        """Test for parser_mod.CmdLineParser
        """
        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        # this should result in error
        self.assertRaises(_Error, parser.parse_args)

        # know attributes to appear in parser output
        global_options = """
            data_query repo_db
            calibRepo clobberConfig clobberOutput clobberVersions debug
            doraise inputRepo loglevel longlog noBackupConfig noVersions
            outputRepo packages processes profile rerun subcommand timeout
            """.split()

        # test for the set of options defined in each command
        args = parser.parse_args(
            """
            list
            """.split())
        list_options = ['show', 'show_headers', 'subparser']
        self.assertEqual(set(vars(args).keys()), set(global_options + list_options))
        self.assertEqual(args.subcommand, 'list')

        args = parser.parse_args(
            """
            build -t cmd
            """.split())
        show_options = ['pipeline_actions', 'show', 'subparser', 'pipeline',
                        'order_pipeline', 'save_pipeline', 'pipeline_dot',
                        'camera_overrides']
        self.assertEqual(set(vars(args).keys()), set(global_options + show_options))
        self.assertEqual(args.subcommand, 'build')

        args = parser.parse_args(
            """
            qgraph -t cmd
            """.split())
        show_options = ['pipeline_actions', 'show', 'subparser', 'pipeline',
                        'order_pipeline', 'save_pipeline', 'pipeline_dot',
                        'qgraph_dot', 'save_qgraph', 'camera_overrides']
        self.assertEqual(set(vars(args).keys()), set(global_options + show_options))
        self.assertEqual(args.subcommand, 'qgraph')

        args = parser.parse_args(
            """
            run -t taskname
            """.split())
        run_options = ['pipeline_actions', 'show', 'subparser', 'pipeline',
                       'order_pipeline', 'save_pipeline', 'pipeline_dot',
                       'qgraph_dot', 'qgraph', 'save_qgraph', 'camera_overrides']
        self.assertEqual(set(vars(args).keys()), set(global_options + run_options))
        self.assertEqual(args.subcommand, 'run')

    def testCmdLineList(self):

        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        # check list subcommand with options
        args = parser.parse_args("list".split())
        self.assertIsNone(args.show)

        args = parser.parse_args("list -p".split())
        self.assertEqual(args.show, ["packages"])

        args = parser.parse_args("list -m".split())
        self.assertEqual(args.show, ["modules"])

        args = parser.parse_args("list -t".split())
        self.assertEqual(args.show, ["tasks"])

        args = parser.parse_args("list -s".split())
        self.assertEqual(args.show, ["super-tasks"])

        args = parser.parse_args("list -s --super-tasks".split())
        self.assertEqual(args.show, ["super-tasks", "super-tasks"])

        args = parser.parse_args("list --packages --modules".split())
        self.assertEqual(args.show, ["packages", "modules"])

        args = parser.parse_args("list --super-tasks --tasks".split())
        self.assertEqual(args.show, ["super-tasks", "tasks"])

    def testCmdLineTasks(self):

        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        PipelineAction = parser_mod._PipelineAction

        # default options
        args = parser.parse_args(
            """
            run -t taskname
            """.split())
        self.assertIsNone(args.calibRepo)
        self.assertFalse(args.clobberConfig)
        self.assertFalse(args.clobberOutput)
        self.assertFalse(args.clobberVersions)
        self.assertFalse(args.debug)
        self.assertFalse(args.doraise)
        self.assertIsNone(args.inputRepo)
        self.assertEqual(args.loglevel, [])
        self.assertFalse(args.longlog)
        self.assertFalse(args.noBackupConfig)
        self.assertFalse(args.noVersions)
        self.assertIsNone(args.outputRepo)
        self.assertEqual(args.processes, 1)
        self.assertIsNone(args.profile)
        self.assertIsNone(args.rerun)
        self.assertIsNone(args.timeout)
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", None, "taskname")])
        self.assertEqual(args.show, [])
        self.assertIsNotNone(args.subparser)
        self.assertIsNone(args.pipeline)

        # bunch of random options
        args = parser.parse_args(
            """
            --calib calibRepo
            --clobber-config
            --clobber-output
            --clobber-versions
            --debug
            --doraise
            --input inputRepo
            --loglevel DEBUG -L component=trace
            --longlog
            --no-backup-config
            --no-versions
            --output outputRepo
            -j 66
            --profile profile.out
            --rerun rerunRepo
            --timeout 10.10
            run -t taskname:label
            --show config
            --show config=Task.*
            -c label.a=b
            -C label:filename1
            -c label.c=d -c label.e=f
            -C label:filename2 -C label:filename3
            """.split())
        self.assertEqual(args.calibRepo, 'calibRepo')
        self.assertTrue(args.clobberConfig)
        self.assertTrue(args.clobberOutput)
        self.assertTrue(args.clobberVersions)
        self.assertTrue(args.debug)
        self.assertTrue(args.doraise)
        self.assertEqual(args.inputRepo, 'inputRepo')
        self.assertEqual(args.loglevel, [(None, 'DEBUG'), ('component', 'TRACE')])
        self.assertTrue(args.longlog)
        self.assertTrue(args.noBackupConfig)
        self.assertTrue(args.noVersions)
        self.assertEqual(args.outputRepo, 'outputRepo')
        self.assertEqual(args.processes, 66)
        self.assertEqual(args.profile, 'profile.out')
        self.assertEqual(args.rerun, 'rerunRepo')
        self.assertEqual(args.timeout, 10.10)
        self.assertEqual(args.show, ['config', 'config=Task.*'])
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", "label", "taskname"),
                                                 PipelineAction("config", "label", "a=b"),
                                                 PipelineAction("configfile", "label", "filename1"),
                                                 PipelineAction("config", "label", "c=d"),
                                                 PipelineAction("config", "label", "e=f"),
                                                 PipelineAction("configfile", "label", "filename2"),
                                                 PipelineAction("configfile", "label", "filename3")])
        self.assertIsNone(args.pipeline)
        self.assertIsNone(args.qgraph)
        self.assertFalse(args.camera_overrides)
        self.assertFalse(args.order_pipeline)
        self.assertIsNone(args.save_pipeline)
        self.assertIsNone(args.save_qgraph)
        self.assertIsNone(args.pipeline_dot)
        self.assertIsNone(args.qgraph_dot)

        # multiple tasks pluse more options (-q should be exclusive with
        # some other options but we do not check it during parsing (yet))
        args = parser.parse_args(
            """
            run
            -p pipeline.pickle
            -g qgraph.pickle
            --camera-overrides
            -t task1
            -t task2:label2
            -t task3
            -t task4
            --show config
            -c task1.a=b
            -C task1:filename1
            -c label2.c=d -c label2.e=f
            -C task3:filename2 -C task3:filename3
            --show config=Task.*
            -C task4:filename4 -c task4.x=y
            --order-pipeline
            --save-pipeline=newpipe.pickle
            --save-qgraph=newqgraph.pickle
            --pipeline-dot pipe.dot
            --qgraph-dot qgraph.dot
            """.split())
        self.assertEqual(args.show, ['config', 'config=Task.*'])
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", None, "task1"),
                                                 PipelineAction("new_task", "label2", "task2"),
                                                 PipelineAction("new_task", None, "task3"),
                                                 PipelineAction("new_task", None, "task4"),
                                                 PipelineAction("config", "task1", "a=b"),
                                                 PipelineAction("configfile", "task1", "filename1"),
                                                 PipelineAction("config", "label2", "c=d"),
                                                 PipelineAction("config", "label2", "e=f"),
                                                 PipelineAction("configfile", "task3", "filename2"),
                                                 PipelineAction("configfile", "task3", "filename3"),
                                                 PipelineAction("configfile", "task4", "filename4"),
                                                 PipelineAction("config", "task4", "x=y")])
        self.assertEqual(args.pipeline, "pipeline.pickle")
        self.assertEqual(args.qgraph, "qgraph.pickle")
        self.assertTrue(args.camera_overrides)
        self.assertTrue(args.order_pipeline)
        self.assertEqual(args.save_pipeline, "newpipe.pickle")
        self.assertEqual(args.save_qgraph, "newqgraph.pickle")
        self.assertEqual(args.pipeline_dot, "pipe.dot")
        self.assertEqual(args.qgraph_dot, "qgraph.dot")

    def testCmdLinePipeline(self):

        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        PipelineAction = parser_mod._PipelineAction

        args = parser.parse_args(
            """
            -p package
            run -p pipeline
            --show config
            --show config=Task.*
            """.split())
        self.assertEqual(args.show, ['config', 'config=Task.*'])
        self.assertEqual(args.pipeline, 'pipeline')
        self.assertEqual(args.pipeline_actions, [])

        args = parser.parse_args("run -p pipeline -t task".split())
        self.assertEqual(args.pipeline, 'pipeline')
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", None, "task")])


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
