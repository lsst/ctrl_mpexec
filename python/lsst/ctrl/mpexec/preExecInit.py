# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = ["PreExecInit"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import abc
import logging
from typing import TYPE_CHECKING

# -----------------------------
#  Imports for other modules --
# -----------------------------

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, LimitedButler
    from lsst.pipe.base import QuantumGraph, TaskFactory

_LOG = logging.getLogger(__name__)


class PreExecInitBase(abc.ABC):
    """Common part of the implementation of PreExecInit classes that does not
    depend on Butler type.

    Parameters
    ----------
    butler : `~lsst.daf.butler.LimitedButler`
        Butler to use.
    taskFactory : `lsst.pipe.base.TaskFactory`
        Ignored and accepted for backwards compatibility.
    extendRun : `bool`
        Whether extend run parameter is in use.
    """

    def __init__(self, butler: LimitedButler, taskFactory: TaskFactory, extendRun: bool):
        self.butler = butler
        self.extendRun = extendRun

    def initialize(
        self,
        graph: QuantumGraph,
        saveInitOutputs: bool = True,
        registerDatasetTypes: bool = False,
        saveVersions: bool = True,
    ) -> None:
        """Perform all initialization steps.

        Convenience method to execute all initialization steps. Instead of
        calling this method and providing all options it is also possible to
        call methods individually.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        saveInitOutputs : `bool`, optional
            If ``True`` (default) then save "init outputs", configurations,
            and package versions to butler.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.
        saveVersions : `bool`, optional
            If ``False`` then do not save package versions even if
            ``saveInitOutputs`` is set to ``True``.
        """
        # register dataset types or check consistency
        self.initializeDatasetTypes(graph, registerDatasetTypes)

        # Save task initialization data or check that saved data
        # is consistent with what tasks would save
        if saveInitOutputs:
            self.saveInitOutputs(graph)
            self.saveConfigs(graph)
            if saveVersions:
                self.savePackageVersions(graph)

    @abc.abstractmethod
    def initializeDatasetTypes(self, graph: QuantumGraph, registerDatasetTypes: bool = False) -> None:
        """Save or check DatasetTypes output by the tasks in a graph.

        Iterates over all DatasetTypes for all tasks in a graph and either
        tries to add them to registry or compares them to existing ones.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.
        registerDatasetTypes : `bool`, optional
            If ``True`` then register dataset types in registry, otherwise
            they must be already registered.

        Raises
        ------
        ValueError
            Raised if existing DatasetType is different from DatasetType
            in a graph.
        KeyError
            Raised if ``registerDatasetTypes`` is ``False`` and DatasetType
            does not exist in registry.
        """
        raise NotImplementedError()

    def saveInitOutputs(self, graph: QuantumGraph) -> None:
        """Write any datasets produced by initializing tasks in a graph.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.

        Raises
        ------
        TypeError
            Raised if the type of existing object in butler is different from
            new data.
        """
        _LOG.debug("Will save InitOutputs for all tasks")
        graph.write_init_outputs(self.butler, skip_existing=self.extendRun)

    def saveConfigs(self, graph: QuantumGraph) -> None:
        """Write configurations for pipeline tasks to butler or check that
        existing configurations are equal to the new ones.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.

        Raises
        ------
        ConflictingDefinitionError
            Raised if existing object in butler is different from new data, or
            if ``extendRun`` is `False` and datasets already exists.
            Content of a butler collection should not be changed if this
            exception is raised.
        """
        graph.write_configs(self.butler, compare_existing=self.extendRun)

    def savePackageVersions(self, graph: QuantumGraph) -> None:
        """Write versions of software packages to butler.

        Parameters
        ----------
        graph : `~lsst.pipe.base.QuantumGraph`
            Execution graph.

        Raises
        ------
        TypeError
            Raised if existing object in butler is incompatible with new data.
        """
        graph.write_packages(self.butler, compare_existing=self.extendRun)


class PreExecInit(PreExecInitBase):
    """Initialization of registry for QuantumGraph execution.

    This class encapsulates all necessary operations that have to be performed
    on butler and registry to prepare them for QuantumGraph execution.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Task factory.
    extendRun : `bool`, optional
        If `True` then do not try to overwrite any datasets that might exist
        in ``butler.run``; instead compare them when appropriate/possible.  If
        `False`, then any existing conflicting dataset will cause a butler
        exception to be raised.
    """

    def __init__(self, butler: Butler, taskFactory: TaskFactory, extendRun: bool = False):
        super().__init__(butler, taskFactory, extendRun)
        self.full_butler = butler
        if self.extendRun and self.full_butler.run is None:
            raise RuntimeError(
                "Cannot perform extendRun logic unless butler is initialized "
                "with a default output RUN collection."
            )

    def initializeDatasetTypes(self, graph: QuantumGraph, registerDatasetTypes: bool = False) -> None:
        # docstring inherited
        if registerDatasetTypes:
            graph.pipeline_graph.register_dataset_types(self.full_butler)
        else:
            graph.pipeline_graph.check_dataset_type_registrations(self.full_butler)


class PreExecInitLimited(PreExecInitBase):
    """Initialization of registry for QuantumGraph execution.

    This class works with LimitedButler and expects that all references in
    QuantumGraph are resolved.

    Parameters
    ----------
    butler : `~lsst.daf.butler.LimitedButler`
        Limited data butler instance.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Task factory.
    """

    def __init__(self, butler: LimitedButler, taskFactory: TaskFactory):
        super().__init__(butler, taskFactory, False)

    def initializeDatasetTypes(self, graph: QuantumGraph, registerDatasetTypes: bool = False) -> None:
        # docstring inherited
        # With LimitedButler we never create or check dataset types.
        pass
