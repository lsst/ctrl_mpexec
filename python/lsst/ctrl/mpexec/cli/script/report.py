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
from collections.abc import Sequence

from astropy.table import Table
from lsst.daf.butler import Butler
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.execution_reports import QuantumGraphExecutionReport
from lsst.pipe.base.quantum_provenance_graph import QuantumProvenanceGraph, Summary


def report(
    butler_config: str,
    qgraph_uri: str,
    full_output_filename: str | None,
    logs: bool = True,
    brief: bool = False,
) -> None:
    """Summarize the produced, missing and expected quanta and
    datasets belonging to an executed quantum graph using the
    `QuantumGraphExecutionReport`.

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
        brief : `bool`
            List only the counts (or data_ids if number of failures < 5). This
            option is good for those who just want to see totals.
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
        if not brief:
            pprint.pprint(error_summary)
            print("\n")
        datasets.pprint_all()
    else:
        report.write_summary_yaml(butler, full_output_filename, do_store_logs=logs)


def report_v2(
    butler_config: str,
    qgraph_uris: Sequence[str],
    collections: Sequence[str] | None,
    where: str,
    full_output_filename: str | None,
    logs: bool = True,
    brief: bool = False,
    curse_failed_logs: bool = False,
) -> None:
    """Summarize the state of executed quantum graph(s), with counts of failed,
    successful and expected quanta, as well as counts of output datasets and
    their visible/shadowed states. Analyze one or more attempts at the same
    processing on the same dataquery-identified "group" and resolve recoveries
    and persistent failures. Identify mismatch errors between groups.

    Parameters
    ----------
    butler_config : `str`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.
    qgraph_uris : `Sequence` [`str`]
            One or more uris to the serialized Quantum Graph(s).
    collections : `Sequence` [`str`] | None`
            Collection(s) associated with said graphs/processing. For use in
            `lsst.daf.butler.registry.queryDatasets` if paring down the query
            would be useful.
    where : `str`
            A "where" string to use to constrain the collections, if passed.
    full_output_filename : `str`
            Output the full pydantic model `QuantumProvenanceGraph.Summary`
            object into a JSON file. This is ideal for error-matching and
            cataloguing tools such as the ones used by Campaign Management
            software and pilots, and for searching and counting specific kinds
            or instances of failures. This option will also print a "brief"
            (counts-only) summary to stdout.
    logs : `bool`
            Store error messages from Butler logs associated with failed quanta
            if `True`.
    brief : `bool`
            Only display short (counts-only) summary on stdout. This includes
            counts and not error messages or data_ids (similar to BPS report).
            This option will still report all `cursed` datasets and `wonky`
            quanta.
    curse_failed_logs : `bool`
            Mark log datasets as `cursed` if they are published in the final
            output collection. Note that a campaign-level collection must be
            used here for `collections` if `curse_failed_logs` is `True`; if
            `lsst.pipe.base.QuantumProvenanceGraph.__resolve_duplicates`
            is run on a list of group-level collections, then each will only
            show log datasets from their own failures as visible and datasets
            from others will be marked as cursed.
    """
    butler = Butler.from_config(butler_config, writeable=False)
    qpg = QuantumProvenanceGraph()
    qgraphs = []
    for qgraph_uri in qgraph_uris:
        qgraph = QuantumGraph.loadUri(qgraph_uri)
        assert qgraph.metadata is not None, "Saved QGs always have metadata."
        qgraphs.append(qgraph)
    # If the most recent graph's timestamp was earlier than any of the
    # previous graphs, raise a RuntimeError.
    for count, qgraph in enumerate(qgraphs):
        if len(qgraphs) > 1:
            previous_graph = qgraphs[count - 1]
            if count > 0 and qgraph.metadata["time"] < previous_graph.metadata["time"]:
                raise RuntimeError(
                    f"""add_new_graph may only be called on graphs
                    which are passed in the order they were
                    created. Please call again, passing your
                    graphs in order. Time of second graph:
                    {qgraph.metadata["time"]} >
                    time of first graph: {previous_graph.metadata["time"]}"""
                )
    qpg.assemble_quantum_provenance_graph(butler, qgraphs, collections, where, curse_failed_logs)
    summary = qpg.to_summary(butler, do_store_logs=logs)
    print_summary(summary, full_output_filename, brief)


def aggregate_reports(
    filenames: Sequence[str], full_output_filename: str | None, brief: bool = False
) -> None:
    """Aggregrate multiple `QuantumProvenanceGraph` summaries on separate
    dataquery-identified groups into one wholistic report. This is intended for
    reports over the same tasks in the same pipeline, after `pipetask report`
    has been resolved over all graphs associated with each group.

    Parameters
    ----------
    filenames : `Sequence[str]`
        The paths to the JSON files produced by `pipetask report` (note: this
        is only compatible with the multi-graph or `--force-v2` option). These
        files correspond to the `QuantumProvenanceGraph.Summary` objects which
        are produced for each group.
    full_output_filename : `str | None`
        The name of the JSON file in which to store the aggregate report, if
        passed. This is passed to `print_summary` at the end of this function.
    brief : `bool = False`
        Only display short (counts-only) summary on stdout. This includes
        counts and not error messages or data_ids (similar to BPS report).
        This option will still report all `cursed` datasets and `wonky`
        quanta. This is passed to `print_summary` at the end of this function.
    """
    summaries: list[Summary] = []
    for filename in filenames:
        with open(filename) as f:
            model = Summary.model_validate_json(f.read())
            summaries.extend([model])
    result = Summary.aggregate(summaries)
    print_summary(result, full_output_filename, brief)


def print_summary(summary: Summary, full_output_filename: str | None, brief: bool = False) -> None:
    """Take a `QuantumProvenanceGraph.Summary` object and write it to a file
    and/or the screen.

    Parameters
    ----------
    summary : `QuantumProvenanceGraph.Summary`
            This `Pydantic` model contains all the information derived from the
            `QuantumProvenanceGraph`.
    full_output_filename : `str | None`
            Name of the JSON file in which to store summary information, if
            passed.
    brief : `bool`
            Only display short (counts-only) summary on stdout. This includes
            counts and not error messages or data_ids (similar to BPS report).
            This option will still report all `cursed` datasets and `wonky`
            quanta.
    """
    quanta_table = []
    failed_quanta_table = []
    wonky_quanta_table = []
    for label, task_summary in summary.tasks.items():
        if task_summary.n_wonky > 0:
            print(
                f"{label} has produced wonky quanta. Recommend processing cease until the issue is resolved."
            )
            for quantum_summary in task_summary.wonky_quanta:
                wonky_quanta_table.append(
                    {
                        "Task": label,
                        "Data ID": quantum_summary.data_id,
                        "Runs and Status": quantum_summary.runs,
                        "Messages": quantum_summary.messages,
                    }
                )
        quanta_table.append(
            {
                "Task": label,
                "Unknown": task_summary.n_unknown,
                "Successful": task_summary.n_successful,
                "Blocked": task_summary.n_blocked,
                "Failed": task_summary.n_failed,
                "Wonky": task_summary.n_wonky,
                "TOTAL": sum(
                    [
                        task_summary.n_successful,
                        task_summary.n_unknown,
                        task_summary.n_blocked,
                        task_summary.n_failed,
                        task_summary.n_wonky,
                    ]
                ),
                "EXPECTED": task_summary.n_expected,
            }
        )
        if task_summary.failed_quanta:
            for quantum_summary in task_summary.failed_quanta:
                failed_quanta_table.append(
                    {
                        "Task": label,
                        "Data ID": quantum_summary.data_id,
                        "Runs and Status": quantum_summary.runs,
                        "Messages": quantum_summary.messages,
                    }
                )
    quanta = Table(quanta_table)
    quanta.pprint_all()
    # Dataset loop
    dataset_table = []
    cursed_datasets = []
    unsuccessful_datasets = {}
    for dataset_type_name, dataset_type_summary in summary.datasets.items():
        dataset_table.append(
            {
                "Dataset": dataset_type_name,
                "Visible": dataset_type_summary.n_visible,
                "Shadowed": dataset_type_summary.n_shadowed,
                "Predicted Only": dataset_type_summary.n_predicted_only,
                "Unsuccessful": dataset_type_summary.n_unsuccessful,
                "Cursed": dataset_type_summary.n_cursed,
                "TOTAL": sum(
                    [
                        dataset_type_summary.n_visible,
                        dataset_type_summary.n_shadowed,
                        dataset_type_summary.n_predicted_only,
                        dataset_type_summary.n_unsuccessful,
                        dataset_type_summary.n_cursed,
                    ]
                ),
                "EXPECTED": dataset_type_summary.n_expected,
            }
        )
        if dataset_type_summary.n_cursed > 0:
            for cursed_dataset in dataset_type_summary.cursed_datasets:
                print(
                    f"{dataset_type_name} has cursed quanta with message(s) {cursed_dataset.messages}. "
                    "Recommend processing cease until the issue is resolved."
                )
                cursed_datasets.append(
                    {
                        "Dataset Type": dataset_type_name,
                        "Producer Data Id": cursed_dataset.producer_data_id,
                    }
                )
        if dataset_type_summary.n_unsuccessful > 0:
            unsuccessful_datasets[dataset_type_name] = dataset_type_summary.unsuccessful_datasets
    datasets = Table(dataset_table)
    datasets.pprint_all()
    curse_table = Table(cursed_datasets)
    # Display wonky quanta
    if wonky_quanta_table:
        print("Wonky Quanta")
        pprint.pprint(wonky_quanta_table)
    # Display cursed datasets
    if cursed_datasets:
        print("Cursed Datasets")
        curse_table.pprint_all()
    if full_output_filename:
        with open(full_output_filename, "w") as stream:
            stream.write(summary.model_dump_json(indent=2))
    else:
        if not brief:
            if failed_quanta_table:
                print("Failed Quanta")
                pprint.pprint(failed_quanta_table)
            if unsuccessful_datasets:
                print("Unsuccessful Datasets")
                pprint.pprint(unsuccessful_datasets)
