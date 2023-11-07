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

from lsst.daf.butler import Butler
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.execution_reports import QuantumGraphExecutionReport


def report(butler_config: str, qgraph_uri: str, output_yaml: str, logs: bool = True) -> None:
    """Write a yaml file summarizing the produced and missing expected datasets
    in a quantum graph.

    Parameters
    ----------
        butler_config : `str`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.
        qgraph_uri : `str`
            The uri of the location of said quantum graph.
        output_yaml : `str`
            The name to be used for the summary yaml file.
        logs : `bool`
            Get butler log datasets for extra information.

    See Also
    --------
        lsst.pipe.base.QuantumGraphExecutionReport.make_reports
        lsst.pipe.base.QuantumGraphExecutionReport.write_summary_yaml
    """
    butler = Butler.from_config(butler_config, writeable=False)
    qgraph = QuantumGraph.loadUri(qgraph_uri)
    report = QuantumGraphExecutionReport.make_reports(butler, qgraph)
    report.write_summary_yaml(butler, output_yaml, do_store_logs=logs)
