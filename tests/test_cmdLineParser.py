# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Simple unit test for parser module.
"""

from argparse import ArgumentParser
import unittest

import lsst.utils.tests
from lsst.daf.butler import CollectionSearch
import lsst.ctrl.mpexec.cmdLineParser as parser_mod


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

        PipelineAction = parser_mod._PipelineAction
        args = parser.parse_args("-t task".split())
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", None, "task")])

        args = parser.parse_args("-t task:label".split())
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", "label", "task")])

        args = parser.parse_args("-t task".split())
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", None, "task")])

        # something that is not syntaxically correct (not a literal)
        with self.assertRaises(_Error):
            parser.parse_args("-t task -s task:CannotEval".split())

        # Literal but of the wrong type
        with self.assertRaises(_Error):
            parser.parse_args("-t task -s task:[1,2]".split())

        # dictionary but not all strings
        with self.assertRaises(_Error):
            parser.parse_args("-t task -s task:{'x':1}".split())

    def testInputCollectionAction(self):
        """Test for a _InputCollectionAction
        """

        parser = _NoExitParser()
        parser.add_argument("-i", dest="input", action=parser_mod._InputCollectionAction, default=[])

        args = parser.parse_args("".split())

        def assertExpressionsEquivalent(a, b):
            """Test that input collection expressions are equivalent after
            being standardized by CollectionSearch.fromExpression.
            """
            self.assertEqual(CollectionSearch.fromExpression(a), CollectionSearch.fromExpression(b))

        assertExpressionsEquivalent(args.input, [])

        args = parser.parse_args("-i coll".split())
        assertExpressionsEquivalent(args.input, ["coll"])

        # collection can appear twice
        args = parser.parse_args("-i coll,coll".split())
        assertExpressionsEquivalent(args.input, ["coll"])

        args = parser.parse_args("-i coll1,coll2,coll3".split())
        assertExpressionsEquivalent(args.input, ["coll1", "coll2", "coll3"])

        args = parser.parse_args("-i coll1 -i coll2 -i coll3".split())
        assertExpressionsEquivalent(args.input, ["coll1", "coll2", "coll3"])

        args = parser.parse_args("-i coll1 -i coll2,coll3".split())
        assertExpressionsEquivalent(args.input, ["coll1", "coll2", "coll3"])

        args = parser.parse_args("-i coll1 -i coll2 -i coll3".split())
        assertExpressionsEquivalent(args.input, ["coll1", "coll2", "coll3"])

        args = parser.parse_args("-i ds:coll".split())
        assertExpressionsEquivalent(args.input, [("coll", "ds")])

        args = parser.parse_args("-i ds1:coll1,ds2:coll2,ds2:coll3".split())
        assertExpressionsEquivalent(args.input, [("coll1", "ds1"), ("coll2", "ds2"), ("coll3", "ds2")])

        args = parser.parse_args("-i coll1,coll2,ds1:coll1,ds2:coll2,coll3".split())
        assertExpressionsEquivalent(args.input, ["coll1", "coll2",
                                                 ("coll1", "ds1"), ("coll2", "ds2"),
                                                 "coll3"])

        args = parser.parse_args("-i coll1 -i coll2 -i ds1:coll1 -i ds2:coll2 -i coll3".split())
        assertExpressionsEquivalent(args.input, ["coll1", "coll2",
                                                 ("coll1", "ds1"), ("coll2", "ds2"),
                                                 "coll3"])

        # use non-empty default
        parser = _NoExitParser()
        parser.add_argument("-i", dest="input", action=parser_mod._InputCollectionAction,
                            default=[("coll", ...)])

        args = parser.parse_args("".split())
        assertExpressionsEquivalent(args.input, ["coll"])

        args = parser.parse_args("-i coll".split())
        assertExpressionsEquivalent(args.input, ["coll"])

        args = parser.parse_args("-i coll1 -i coll2 -i coll3".split())
        assertExpressionsEquivalent(args.input, ["coll", "coll1", "coll2", "coll3"])

    def testCmdLineParser(self):
        """Test for parser_mod.CmdLineParser
        """
        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        # this should result in error
        self.assertRaises(_Error, parser.parse_args)

        # know attributes to appear in parser output for all subcommands
        common_options = "loglevel longlog enableLsstDebug subcommand subparser".split()

        # test for the set of options defined in each command
        args = parser.parse_args(
            """
            build -t cmd
            """.split())
        build_options = """pipeline pipeline_actions order_pipeline
                        save_pipeline pipeline_dot show""".split()
        self.assertEqual(set(vars(args).keys()), set(common_options + build_options))
        self.assertEqual(args.subcommand, 'build')

        args = parser.parse_args(
            """
            qgraph -t cmd
            """.split())
        qgraph_options = build_options + """qgraph data_query butler_config
                         input output skip_existing output_run extend_run
                         replace_run prune_replaced
                         save_qgraph qgraph_dot save_single_quanta""".split()
        self.assertEqual(set(vars(args).keys()), set(common_options + qgraph_options))
        self.assertEqual(args.subcommand, 'qgraph')

        args = parser.parse_args(
            """
            run -t taskname
            """.split())
        run_options = qgraph_options + """register_dataset_types skip_init_writes
                      init_only processes profile timeout doraise graph_fixup""".split()
        self.assertEqual(set(vars(args).keys()), set(common_options + run_options))
        self.assertEqual(args.subcommand, 'run')

    def testCmdLineTasks(self):

        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        PipelineAction = parser_mod._PipelineAction

        # default options
        args = parser.parse_args(
            """
            run -t taskname
            """.split())
        self.assertFalse(args.enableLsstDebug)
        self.assertFalse(args.doraise)
        self.assertEqual(args.input, [])
        self.assertEqual(args.loglevel, [])
        self.assertFalse(args.longlog)
        self.assertEqual(args.output, None)
        self.assertEqual(args.output_run, None)
        self.assertEqual(args.extend_run, False)
        self.assertEqual(args.replace_run, False)
        self.assertEqual(args.processes, 1)
        self.assertIsNone(args.profile)
        self.assertIsNone(args.timeout)
        self.assertIsNone(args.graph_fixup)
        self.assertEqual(args.pipeline_actions, [PipelineAction("new_task", None, "taskname")])
        self.assertEqual(args.show, [])
        self.assertIsNotNone(args.subparser)
        self.assertIsNone(args.pipeline)

        # bunch of random options
        args = parser.parse_args(
            """
            run
            --debug
            --doraise
            --input inputColl
            --loglevel DEBUG -L component=trace
            --longlog
            --output outputColl
            -j 66
            --profile profile.out
            --timeout 10.10
            -t taskname:label
            --show config
            --show config=Task.*
            -c label:a=b
            -C label:filename1
            -c label:c=d -c label:e=f
            -C label:filename2 -C label:filename3
            --skip-existing
            """.split())
        self.assertTrue(args.enableLsstDebug)
        self.assertTrue(args.doraise)
        self.assertEqual(args.input, [("inputColl", ...)])
        self.assertEqual(args.loglevel, [(None, 'DEBUG'), ('component', 'TRACE')])
        self.assertTrue(args.longlog)
        self.assertEqual(args.output, "outputColl")
        self.assertEqual(args.processes, 66)
        self.assertEqual(args.profile, 'profile.out')
        self.assertEqual(args.timeout, 10.10)
        self.assertIsNone(args.graph_fixup)
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
        self.assertFalse(args.order_pipeline)
        self.assertIsNone(args.save_pipeline)
        self.assertIsNone(args.save_qgraph)
        self.assertIsNone(args.pipeline_dot)
        self.assertIsNone(args.qgraph_dot)
        self.assertTrue(args.skip_existing)

        # multiple tasks plus more options (-q should be exclusive with
        # some other options but we do not check it during parsing (yet))
        args = parser.parse_args(
            """
            run
            -p pipeline.yaml
            -g qgraph.pickle
            -t task1
            -t task2:label2
            -t task3
            -t task4
            --show config
            -c task1:a=b
            -C task1:filename1
            -c label2:c=d -c label2:e=f
            -C task3:filename2 -C task3:filename3
            --show config=Task.*
            -C task4:filename4 -c task4:x=y
            --order-pipeline
            --save-pipeline=newpipe.yaml
            --save-qgraph=newqgraph.pickle
            --pipeline-dot pipe.dot
            --qgraph-dot qgraph.dot
            --graph-fixup lsst.ctrl.mpexec.Fixup
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
        self.assertEqual(args.pipeline, "pipeline.yaml")
        self.assertEqual(args.qgraph, "qgraph.pickle")
        self.assertTrue(args.order_pipeline)
        self.assertEqual(args.save_pipeline, "newpipe.yaml")
        self.assertEqual(args.save_qgraph, "newqgraph.pickle")
        self.assertEqual(args.pipeline_dot, "pipe.dot")
        self.assertEqual(args.qgraph_dot, "qgraph.dot")
        self.assertEqual(args.graph_fixup, "lsst.ctrl.mpexec.Fixup")

    def testCmdLinePipeline(self):

        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        PipelineAction = parser_mod._PipelineAction

        args = parser.parse_args(
            """
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
