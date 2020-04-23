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

__all__ = ['ExecutionGraphFixup']

from typing import Any, Iterable, Sequence, Tuple, Union

from lsst.pipe.base import QuantumIterData
from .executionGraphFixup import ExecutionGraphFixup


class ExecFixupDataId(ExecutionGraphFixup):
    """Implementation of ExecutionGraphFixup for ordering of tasks based
    on DataId values.

    This class is a trivial implementation mostly useful as an example,
    though it can be used to make actual fixup instances by defining
    a method that instantiates it, e.g.::

        # lsst/ap/verify/ci_fixup.py

        from lsst.ctrl.mpexec.execFixupDataId import ExecFixupDataId

        def assoc_fixup():
            return ExecFixupDataId(taskLabel="ap_assoc",
                                   dimensions=("visit", "detector"))


    and then executing pipetask::

        pipetask run --graph-fixup=lsst.ap.verify.ci_fixup.assoc_fixup ...

    This will add new dependencies between quanta executed by the task with
    label "ap_assoc". Quanta with higher visit number will depend on quanta
    with lower visit number and their execution will wait until lower visit
    number finishes.

    Parameters
    ----------
    taskLabel : `str`
        The label of the task for which to add dependencies.
    dimensions : `str` or sequence [`str`]
        One or more dimension names, quanta execution will be ordered
        according to values of these dimensions.
    reverse : `bool`, optional
        If `False` (default) then quanta with higher values of dimensions
        will be executed after quanta with lower values, otherwise the order
        is reversed.
    """

    def __init__(self, taskLabel: str, dimensions: Union[str, Sequence[str]], reverse: bool = False):
        self.taskLabel = taskLabel
        self.dimensions = dimensions
        self.reverse = reverse
        if isinstance(self.dimensions, str):
            self.dimensions = (self.dimensions, )
        else:
            self.dimensions = tuple(self.dimensions)

    def _key(self, qdata: QuantumIterData) -> Tuple[Any, ...]:
        """Produce comparison key for quantum data.

        Parameters
        ----------
        qdata : `QuantumIterData`

        Returns
        -------
        key : `tuple`
        """
        dataId = qdata.quantum.dataId
        key = tuple(dataId[dim] for dim in self.dimensions)
        return key

    def fixupQuanta(self, quanta: Iterable[QuantumIterData]) -> Iterable[QuantumIterData]:
        # Docstring inherited from ExecutionGraphFixup.fixupQuanta
        quanta = list(quanta)
        # Index task quanta by the key
        keyQuanta = {}
        for qdata in quanta:
            if qdata.taskDef.label == self.taskLabel:
                key = self._key(qdata)
                keyQuanta.setdefault(key, []).append(qdata)
        if not keyQuanta:
            raise ValueError(f"Cannot find task with label {self.taskLabel}")
        # order keys
        keys = sorted(keyQuanta.keys(), reverse=self.reverse)
        # for each quanta in a key add dependency to all quanta in a preceding key
        for prev_key, key in zip(keys, keys[1:]):
            prev_indices = frozenset(qdata.index for qdata in keyQuanta[prev_key])
            for qdata in keyQuanta[key]:
                qdata.dependencies |= prev_indices
        return quanta
