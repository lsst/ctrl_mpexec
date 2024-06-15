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
import pprint

import yaml
from astropy.table import Table
from lsst.daf.butler import Butler
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.execution_reports import QuantumGraphExecutionReport


def report(
    butler_config: str,
    qgraph_uri: str,
    full_output_filename: str | None,
    logs: bool = True,
    show_errors: bool = False,
) -> None:
    """Summarize the produced and missing expected dataset in a quantum graph.

    Parameters
    ----------
        butler_config : `str`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.
        qgraph_uri : `str`
            The uri of the location of said quantum graph.
        full_output_filename : `str`
            Output the full summary report to a yaml file (named herein).
            Each data id and error message is keyed to a quantum graph node id.
            A convenient output format for error-matching and cataloguing tools
            such as the ones in the Campaign Management database. If this is
            not included, quanta and dataset information will be printed to the
            command-line instead.
        logs : `bool`
            Get butler log datasets for extra information (error messages).
        show_errors : `bool`
            If no output yaml is provided, print error messages to the
            command-line along with the report. By default, these messages and
            their associated data ids are stored in a yaml file with format
            `{run timestamp}_err.yaml` in the working directory instead.
    """
    butler = Butler.from_config(butler_config, writeable=False)
    qgraph = QuantumGraph.loadUri(qgraph_uri)
    report = QuantumGraphExecutionReport.make_reports(butler, qgraph)
    if not full_output_filename:
        # this is the option to print to the command-line
        summary_dict = report.to_summary_dict(butler, logs, human_readable=True)
        dataset_table_rows = []
        data_products = []
        quanta_summary = []
        error_summary = []
        for task in summary_dict.keys():
            for data_product in summary_dict[task]["outputs"]:
                dataset_table_rows.append(summary_dict[task]["outputs"][data_product])
                data_products.append(data_product)

            if len(summary_dict[task]["failed_quanta"]) > 5:
                quanta_summary.append(
                    {
                        "Task": task,
                        "Failed": len(summary_dict[task]["failed_quanta"]),
                        "Blocked": summary_dict[task]["n_quanta_blocked"],
                        "Succeeded": summary_dict[task]["n_succeeded"],
                        "Expected": summary_dict[task]["n_expected"],
                    }
                )
            else:
                quanta_summary.append(
                    {
                        "Task": task,
                        "Failed": summary_dict[task]["failed_quanta"],
                        "Blocked": summary_dict[task]["n_quanta_blocked"],
                        "Succeeded": summary_dict[task]["n_succeeded"],
                        "Expected": summary_dict[task]["n_expected"],
                    }
                )
            if "errors" in summary_dict[task].keys():
                error_summary.append({task: summary_dict[task]["errors"]})
        quanta = Table(quanta_summary)
        datasets = Table(dataset_table_rows)
        datasets.add_column(data_products, index=0, name="DatasetType")
        quanta.pprint_all()
        print("\n")
        if show_errors:
            pprint.pprint(error_summary)
            print("\n")
        else:
            assert qgraph.metadata is not None, "Saved QGs always have metadata."
            collection = qgraph.metadata["output_run"]
            collection = str(collection)
            run_name = collection.split("/")[-1]
            with open(f"{run_name}_err.yaml", "w") as stream:
                yaml.safe_dump(error_summary, stream)
        datasets.pprint_all()
    else:
        report.write_summary_yaml(butler, full_output_filename, do_store_logs=logs)
