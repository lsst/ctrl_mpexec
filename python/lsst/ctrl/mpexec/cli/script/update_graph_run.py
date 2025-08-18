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

import logging

from lsst.pipe.base.quantum_graph import PredictedQuantumGraphComponents
from lsst.resources import ResourcePathExpression

_LOG = logging.getLogger(__name__)


def update_graph_run(
    input_graph: ResourcePathExpression,
    run: str,
    output_graph: ResourcePathExpression,
    metadata_run_key: str = "output_run",
    update_graph_id: bool = False,
) -> None:
    """Update quantum graph with new output run name and dataset IDs and save
    updated graph to a file.

    Parameters
    ----------
    input_graph : `~lsst.resources.ResourcePathExpression`
        Location of a file with existing quantum graph.
    run : `str`
        Collection name, if collection exists it must be of ``RUN`` type.
    output_graph : `~lsst.resources.ResourcePathExpression`
        Location to store updated quantum graph.
    metadata_run_key : `str`, optional
        Ignored (overriding warns).
    update_graph_id : `bool`
        Ignored (overriding warns).
    """
    qgc = PredictedQuantumGraphComponents.read_execution_quanta(input_graph)
    if metadata_run_key and metadata_run_key != "output_run":
        _LOG.warning("--metadata-run-key is now ignored.")
    if update_graph_id:
        _LOG.warning("--update-graph-id is now ignored.")
    qgc.update_output_run(run)
    qgc.write(output_graph)
