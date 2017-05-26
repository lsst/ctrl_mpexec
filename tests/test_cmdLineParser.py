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

    def testAppendFlattenAction(self):
        """Test for a _AppendFlattenAction class
        """

        # collects all option values as strings
        parser = ArgumentParser()
        parser.add_argument("-c", dest='config_overrides', nargs="+",
                            action=parser_mod._AppendFlattenAction)

        args = parser.parse_args([])
        self.assertTrue(hasattr(args, 'config_overrides'))

        args = parser.parse_args("-c 1".split())
        self.assertTrue(hasattr(args, 'config_overrides'))
        self.assertTrue(isinstance(args.config_overrides, list))
        self.assertEqual(args.config_overrides, ["1"])

        args = parser.parse_args("-c 1 2 3".split())
        self.assertEqual(args.config_overrides, ["1", "2", "3"])

        args = parser.parse_args("-c 1 2 3 -c 4 5 6".split())
        self.assertEqual(args.config_overrides, ["1", "2", "3", "4", "5", "6"])

        # convert optin values
        parser = ArgumentParser()
        parser.add_argument("-c", dest='config_overrides', nargs="+",
                            action=parser_mod._AppendFlattenAction, type=int)

        args = parser.parse_args("-c 1 2 3 -c 4 5 6".split())
        self.assertEqual(args.config_overrides, [1, 2, 3, 4, 5, 6])

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

    def testConfigOverrides(self):
        """Test for a _config_override and _config_file conversions
        """
        parser = ArgumentParser()
        parser.add_argument("-c", dest='config_overrides', nargs="+",
                            action=parser_mod._AppendFlattenAction,
                            type=parser_mod._config_override)
        parser.add_argument("-C", dest='config_overrides', nargs="+",
                            action=parser_mod._AppendFlattenAction,
                            type=parser_mod._config_file)

        Override = parser_mod._Override
        args = parser.parse_args("-c a=b".split())
        self.assertEqual(args.config_overrides, [Override("override", "a=b")])

        args = parser.parse_args("-C filename".split())
        self.assertEqual(args.config_overrides, [Override("file", "filename")])

        args = parser.parse_args("-c a=b c=d -C filename1 filename2 -c e=f -C filename3".split())
        self.assertEqual(args.config_overrides, [Override("override", "a=b"),
                                                 Override("override", "c=d"),
                                                 Override("file", "filename1"),
                                                 Override("file", "filename2"),
                                                 Override("override", "e=f"),
                                                 Override("file", "filename3"),
                                                 ])

    def testCmdLineParser(self):
        """Test for parser_mod.CmdLineParser
        """
        parser = parser_mod.makeParser(parser_class=_NoExitParser)

        # this should result in error
        self.assertRaises(_Error, parser.parse_args)

        # know attributes to appear in parser output
        global_options = """
            data_query
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
            show cmd
            """.split())
        show_options = ['taskname', 'show', 'config_overrides', 'subparser']
        self.assertEqual(set(vars(args).keys()), set(global_options + show_options))
        self.assertEqual(args.subcommand, 'show')

        args = parser.parse_args(
            """
            run taskname
            """.split())
        run_options = ['taskname', 'show', 'config_overrides', 'subparser']
        self.assertEqual(set(vars(args).keys()), set(global_options + run_options))
        self.assertEqual(args.subcommand, 'run')

        # default options
        args = parser.parse_args(
            """
            run taskname
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
        self.assertEqual(args.taskname, 'taskname')
        self.assertEqual(args.show, [])
        self.assertEqual(args.config_overrides, [])
        self.assertIsNotNone(args.subparser)

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
            --loglevel DEBUG component=trace
            --longlog
            --no-backup-config
            --no-versions
            --output outputRepo
            -j 66
            --profile profile.out
            --rerun rerunRepo
            --timeout 10.10
            run taskname
            --show config config=Task.*
            -c a=b
            -C filename1
            -c c=d -c e=f
            -C filename2 -C filename3
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
        self.assertEqual(args.taskname, 'taskname')
        self.assertEqual(args.show, ['config', 'config=Task.*'])
        self.assertEqual(len(args.config_overrides), 6)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
