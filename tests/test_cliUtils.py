# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit tests for the daf_butler shared CLI options.
"""

import unittest

from lsst.ctrl.mpexec.cli.utils import _PipelineAction, makePipelineActions


class PipelineActionTestCase(unittest.TestCase):
    def test_makePipelineActions(self):
        """Test converting each CLI option flag to its associated pipeline
        action type."""
        self.assertEqual(
            makePipelineActions(["-t", "foo"]), [_PipelineAction(action="new_task", label=None, value="foo")]
        )
        self.assertEqual(
            makePipelineActions(["--task", "foo"]),
            [_PipelineAction(action="new_task", label=None, value="foo")],
        )
        self.assertEqual(
            makePipelineActions(["--delete", "foo"]),
            [_PipelineAction(action="delete_task", label="foo", value="")],
        )
        self.assertEqual(
            makePipelineActions(["-c", "task:addend=100"]),
            [_PipelineAction(action="config", label="task", value="addend=100")],
        )
        self.assertEqual(
            makePipelineActions(["--config", "task:addend=100"]),
            [_PipelineAction(action="config", label="task", value="addend=100")],
        )
        self.assertEqual(
            makePipelineActions(["-C", "task:filename"]),
            [_PipelineAction(action="configfile", label="task", value="filename")],
        )
        self.assertEqual(
            makePipelineActions(["--config-file", "task:filename"]),
            [_PipelineAction(action="configfile", label="task", value="filename")],
        )
        self.assertEqual(
            makePipelineActions(["--instrument", "foo"]),
            [_PipelineAction(action="add_instrument", label=None, value="foo")],
        )
        self.assertEqual(
            makePipelineActions(["--instrument", "foo"]),
            [_PipelineAction(action="add_instrument", label=None, value="foo")],
        )

    def test_nonActions(self):
        """Test that args with a flag that does not represent an action works;
        that arg should be ignored."""
        self.assertEqual(makePipelineActions(["-n"]), [])
        self.assertEqual(
            makePipelineActions(["-n", "-t", "foo"]),
            [_PipelineAction(action="new_task", label=None, value="foo")],
        )

    def test_multipleActions(self):
        """Test args with multiple actions, with non-actions mixed in."""
        self.assertEqual(
            makePipelineActions(
                [
                    "--foo",
                    "bar",
                    "-C",
                    "task:filename",
                    "-x",
                    "-c",
                    "task:addend=100",
                    "--instrument",
                    "instr",
                ]
            ),
            [
                _PipelineAction(action="configfile", label="task", value="filename"),
                _PipelineAction(action="config", label="task", value="addend=100"),
                _PipelineAction(action="add_instrument", label=None, value="instr"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
