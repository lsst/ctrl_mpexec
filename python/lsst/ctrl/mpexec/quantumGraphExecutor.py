# This file is part of ctrl_mpexec.
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

__all__ = ["QuantumExecutor", "QuantumGraphExecutor"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from abc import ABC, abstractmethod

# -----------------------------
#  Imports for other modules --
# -----------------------------


class QuantumExecutor(ABC):
    """Class which abstracts execution of a single Quantum.

    In general implementation should not depend on execution model and
    execution should always happen in-process. Main reason for existence
    of this class is to provide do-nothing implementation that can be used
    in the unit tests.
    """

    @abstractmethod
    def execute(self, taskDef, quantum, butler):
        """Execute single quantum.

        Parameters
        ----------
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.
        quantum : `~lsst.daf.butler.Quantum`
            Quantum for this execution.
        butler : `~lsst.daf.butler.Butler`
            Data butler instance
        """
        raise NotImplementedError


class QuantumGraphExecutor(ABC):
    """Class which abstracts QuantumGraph execution.

    Any specific execution model is implemented in sub-class by overriding
    the `execute` method.
    """

    @abstractmethod
    def execute(self, graph, butler):
        """Execute whole graph.

        Implementation of this method depends on particular execution model
        and it has to be provided by a subclass. Execution model determines
        what happens here; it can be either actual running of the task or,
        for example, generation of the scripts for delayed batch execution.

        Parameters
        ----------
        graph : `~lsst.pip.base.QuantumGraph`
            Execution graph.
        butler : `~lsst.daf.butler.Butler`
            Data butler instance
        """
        raise NotImplementedError
