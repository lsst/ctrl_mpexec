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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import click

from lsst.daf.butler.cli.utils import MWOptionDecorator, MWPath, split_commas, unwrap


butler_config_option = MWOptionDecorator("-b", "--butler-config",
                                         help="Location of the gen3 butler/registry config file.")


data_query_option = MWOptionDecorator("-d", "--data-query",
                                      help="User data selection expression.",
                                      metavar="QUERY")


debug_option = MWOptionDecorator("--debug",
                                 help="Enable debugging output using lsstDebug facility (imports debug.py).",
                                 is_flag=True)


delete_option = MWOptionDecorator("--delete",
                                  callback=split_commas,
                                  help="Delete task with given label from pipeline.",
                                  multiple=True)


do_raise_option = MWOptionDecorator("--do-raise",
                                    help="Raise an exception on error. (else log a message and continue?)",
                                    is_flag=True)


extend_run_option = MWOptionDecorator("--extend-run",
                                      help=unwrap("""Instead of creating a new RUN collection, insert datasets
                                                  into either the one given by --output-run (if provided) or
                                                  the first child collection of - -output(which must be of
                                                  type RUN)."""),
                                      is_flag=True)


graph_fixup_option = MWOptionDecorator("--graph-fixup",
                                       help=unwrap("""Name of the class or factory method which makes an
                                                   instance used for execution graph fixup."""))


init_only_option = MWOptionDecorator("--init-only",
                                     help=unwrap("""Do not actually run; just register dataset types and/or
                                                 save init outputs. """),
                                     is_flag=True)


input_option = MWOptionDecorator("-i", "--input",
                                 callback=split_commas,
                                 default=list(),
                                 help=unwrap("""Comma-separated names of the input collection(s)."""),
                                 metavar="COLLECTION",
                                 multiple=True)

no_versions_option = MWOptionDecorator("--no-versions",
                                       help="Do not save or check package versions.",
                                       is_flag=True)


order_pipeline_option = MWOptionDecorator("--order-pipeline",
                                          help=unwrap("""Order tasks in pipeline based on their data
                                          dependencies, ordering is performed as last step before saving or
                                          executing pipeline."""),
                                          is_flag=True)


output_option = MWOptionDecorator("-o", "--output",
                                  help=unwrap("""Name of the output CHAINED collection. This may either be an
                                              existing CHAINED collection to use as both input and output
                                              (incompatible with --input), or a new CHAINED collection created
                                              to include all inputs (requires --input). In both cases, the
                                              collection's children will start with an output RUN collection
                                              that directly holds all new datasets (see --output-run)."""),
                                  metavar="COLL")


output_run_option = MWOptionDecorator("--output-run",
                                      help=unwrap("""Name of the new output RUN collection. If not provided
                                                  then --output must be provided and a new RUN collection will
                                                  be created by appending a timestamp to the value passed with
                                                  --output. If this collection already exists then
                                                  --extend-run must be passed."""),
                                      metavar="COLL")


pipeline_option = MWOptionDecorator("-p", "--pipeline",
                                    help="Location of a pipeline definition file in YAML format.",
                                    type=MWPath(file_okay=True, dir_okay=False, readable=True))


pipeline_dot_option = MWOptionDecorator("--pipeline-dot",
                                        help=unwrap(""""Location for storing GraphViz DOT representation of a
                                                    pipeline."""),
                                        type=MWPath(writable=True, file_okay=True, dir_okay=False))


profile_option = MWOptionDecorator("--profile",
                                   help="Dump cProfile statistics to file name.",
                                   type=MWPath(file_okay=True, dir_okay=False))


prune_replaced_option = MWOptionDecorator("--prune-replaced",
                                          help=unwrap("""Delete the datasets in the collection replaced by
                                                      --replace-run, either just from the datastore
                                                      ('unstore') or by removing them and the RUN completely
                                                      ('purge'). Requires --replace-run."""),
                                          type=click.Choice(("unstore", "purge"), case_sensitive=False))


qgraph_option = MWOptionDecorator("-g", "--qgraph",
                                  help=unwrap("""Location for a serialized quantum graph definition (pickle
                                              file). If this option is given then all input data options and
                                              pipeline-building options cannot be used.  Can be a URI."""))


qgraph_dot_option = MWOptionDecorator("--qgraph-dot",
                                      help=unwrap("""Location for storing GraphViz DOT representation of a
                                                  quantum graph."""),
                                      type=MWPath(writable=True, file_okay=True, dir_okay=False))


register_dataset_types_option = MWOptionDecorator("--register-dataset-types",
                                                  help=unwrap("""Register DatasetTypes that do not already
                                                              exist in the Registry."""),
                                                  is_flag=True)


replace_run_option = MWOptionDecorator("--replace-run",
                                       help=unwrap("""Before creating a new RUN collection in an existing
                                                   CHAINED collection, remove the first child collection
                                                   (which must be of type RUN). This can be used to repeatedly
                                                   write to the same (parent) collection during development,
                                                   but it does not delete the datasets associated with the
                                                   replaced run unless --prune-replaced is also passed.
                                                   Requires --output, and incompatible with --extend-run."""),
                                       is_flag=True)


save_pipeline_option = MWOptionDecorator("-s", "--save-pipeline",
                                         help=unwrap("""Location for storing resulting pipeline definition in
                                                     YAML format."""),
                                         type=MWPath(dir_okay=False, file_okay=True, writable=True))

save_qgraph_option = MWOptionDecorator("-q", "--save-qgraph",
                                       help=unwrap("""URI location for storing a serialized quantum graph
                                                   definition (pickle file)."""))


save_single_quanta_option = MWOptionDecorator("--save-single-quanta",
                                              help=unwrap("""Format string of locations for storing individual
                                                          quantum graph definition (pickle files). The curly
                                                          brace {} in the input string will be replaced by a
                                                          quantum number. Can be a URI."""))


show_option = MWOptionDecorator("--show",
                                callback=split_commas,
                                help=unwrap("""Dump various info to standard output. Possible items are:
                                            `config`, `config=[Task::]<PATTERN>` or
                                            `config=[Task::]<PATTERN>:NOIGNORECASE` to dump configuration
                                            fields possibly matching given pattern and/or task label;
                                            `history=<FIELD>` to dump configuration history for a field, field
                                            name is specified as [Task::]<PATTERN>; `dump-config`,
                                            `dump-config=Task` to dump complete configuration for a task given
                                            its label or all tasks; `pipeline` to show pipeline composition;
                                            `graph` to show information about quanta; `workflow` to show
                                            information about quanta and their dependency; `tasks` to show
                                            task composition; `uri` to show predicted dataset URIs of
                                            quanta"""),
                                metavar="ITEM|ITEM=VALUE",
                                multiple=True)


skip_existing_option = MWOptionDecorator("--skip-existing",
                                         help=unwrap("""If all Quantum outputs already exist in the output RUN
                                                     collection then that Quantum will be excluded from the
                                                     QuantumGraph. Requires the 'run` command's `--extend-run`
                                                     flag to be set."""),
                                         is_flag=True)


clobber_partial_outputs_option = MWOptionDecorator("--clobber-partial-outputs",
                                                   help=unwrap("""Remove incomplete outputs from previous
                                                               execution of the same quantum before new
                                                               execution."""),
                                                   is_flag=True)


skip_init_writes_option = MWOptionDecorator("--skip-init-writes",
                                            help=unwrap("""Do not write collection-wide 'init output' datasets
                                                        (e.g.schemas)."""),
                                            is_flag=True)


task_option = MWOptionDecorator("-t", "--task",
                                callback=split_commas,
                                help=unwrap("""Task name to add to pipeline, must be a fully qualified task
                                            name. Task name can be followed by colon and label name, if label
                                            is not given then task base name (class name) is used as
                                            label."""),
                                metavar="TASK[:LABEL]",
                                multiple=True)


timeout_option = MWOptionDecorator("--timeout",
                                   type=click.IntRange(min=0),
                                   help="Timeout for multiprocessing; maximum wall time (sec).")


start_method_option = MWOptionDecorator("--start-method",
                                        default=None,
                                        type=click.Choice(["spawn", "fork", "forkserver"]),
                                        help="Multiprocessing start method, default is platform-specific.")


fail_fast_option = MWOptionDecorator("--fail-fast",
                                     help=unwrap("""Stop processing at first error, default is to process
                                                 as many tasks as possible."""),
                                     is_flag=True)
