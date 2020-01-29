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

"""Module defining `makeParser` factory method.
"""

__all__ = ["makeParser"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from argparse import Action, ArgumentParser, RawDescriptionHelpFormatter
import collections
import copy
import re
import textwrap

# -----------------------------
#  Imports for other modules --
# -----------------------------

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# Class which determines an action that needs to be performed
# when building pipeline, its attributes are:
#   action: the name of the action, e.g. "new_task", "delete_task"
#   label:  task label, can be None if action does not require label
#   value:  argument value excluding task label.
_PipelineAction = collections.namedtuple("_PipelineAction", "action,label,value")


class _PipelineActionType:
    """Class defining a callable type which converts strings into
    _PipelineAction instances.

    Parameters
    ----------
    action : str
        Name of the action, will become `action` attribute of instance.
    regex : str
        Regular expression for argument value, it can define groups 'label'
        and 'value' which will become corresponding attributes of a
        returned instance.
    """

    def __init__(self, action, regex='.*', valueType=str):
        self.action = action
        self.regex = re.compile(regex)
        self.valueType = valueType

    def __call__(self, value):
        match = self.regex.match(value)
        if not match:
            raise TypeError("Unrecognized option syntax: " + value)
        # get "label" group or use None as label
        try:
            label = match.group("label")
        except IndexError:
            label = None
        # if "value" group is not defined use whole string
        try:
            value = match.group("value")
        except IndexError:
            pass
        value = self.valueType(value)
        return _PipelineAction(self.action, label, value)

    def __repr__(self):
        """String representation of this class.

        argparse can use this for some error messages, default implementation
        makes those messages incomprehensible.
        """
        return f"_PipelineActionType(action={self.action})"


_ACTION_ADD_TASK = _PipelineActionType("new_task", "(?P<value>[^:]+)(:(?P<label>.+))?")
_ACTION_DELETE_TASK = _PipelineActionType("delete_task", "(?P<value>)(?P<label>.+)")
_ACTION_CONFIG = _PipelineActionType("config", "(?P<label>.+):(?P<value>.+=.+)")
_ACTION_CONFIG_FILE = _PipelineActionType("configfile", "(?P<label>.+):(?P<value>.+)")
_ACTION_ADD_INSTRUMENT = _PipelineActionType("add_instrument", "(?P<value>[^:]+)")


class _LogLevelAction(Action):
    """Action class which collects logging levels.

    This action class collects arguments in the form "LEVEL" or
    "COMPONENT=LEVEL" where LEVEL is the name of the logging level (case-
    insensitive). It converts the series of arguments into the list of
    tuples (COMPONENT, LEVEL). If component name is missing then first
    item in tuple is set to `None`. Second item in tuple is converted to
    upper case.
    """

    permittedLevels = set(['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'])

    def __call__(self, parser, namespace, values, option_string=None):
        """Re-implementation of the base class method.

        See `argparse.Action` documentation for parameter description.
        """
        dest = getattr(namespace, self.dest)
        if dest is None:
            dest = []
            setattr(namespace, self.dest, dest)

        component, _, levelStr = values.partition("=")
        if not levelStr:
            levelStr, component = component, None
        logLevelUpr = levelStr.upper()
        if logLevelUpr not in self.permittedLevels:
            parser.error("loglevel=%s not one of %s" % (levelStr, tuple(self.permittedLevels)))
        dest.append((component, logLevelUpr))


class _InputCollectionAction(Action):
    """Action class which collects input collection names.

    This action type accepts string values in format:

        value :== collection[,collection[...]]
        collection :== [dataset_type:]collection_name

    and converts value into a dictionary whose keys a dataset type names
    (or empty string when dataset type name is missing) and values are
    ordered lists of collection names. Values from multiple arguments are
    all collected into the same dictionary. Resulting list of collections
    could contain multiple instances of the same collection name if it
    appears multiple time on command line.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        """Re-implementation of the base class method.

        See `argparse.Action` documentation for parameter description.
        """
        dest = getattr(namespace, self.dest, {})
        # In case default is set to a dict (empty or not) we want to use
        # new deep copy of that dictionary as initial value to avoid
        # modifying default value.
        if dest is self.default:
            dest = copy.deepcopy(dest)

        # split on commas, collection can be preceeded by dataset type
        for collstr in values.split(","):
            dsType, sep, coll = collstr.partition(':')
            if not sep:
                dsType, coll = "", dsType
            dest.setdefault(dsType, []).append(coll)

        setattr(namespace, self.dest, dest)


def _outputCollectionType(value):
    """Special argument type for input collections.

    Accepts string vlues in format:

        value :== collection[,collection[...]]
        collection :== [dataset_type:]collection_name

    and converts value into a dictionary whose keys a dataset type names
    (or empty string when dataset type name is missing) and values are
    collection names.

    Parameters
    ----------
    value : `str`
        Value of the command line option

    Returns
    -------
    `dict`

    Raises
    ------
    `ValueError` if there is more than one collection per dataset type.
    """
    res = {}
    for collstr in value.split(","):
        dsType, sep, coll = collstr.partition(':')
        if not sep:
            dsType, coll = "", dsType
        if dsType in res:
            raise ValueError("multiple collection names: " + value)
        res[dsType] = coll
    return res


_EPILOG = """\
Notes:
  * many options can appear multiple times; all values are used, in order
    left to right
  * @file reads command-line options from the specified file:
    * data may be distributed among multiple lines (e.g. one option per line)
    * data after # is treated as a comment and ignored
    * blank lines and lines starting with # are ignored
"""


def _makeButlerOptions(parser):
    """Add a set of options for data butler to a parser.

    Parameters
    ----------
    parser : `argparse.ArgumentParser`
    """
    group = parser.add_argument_group("Data repository and selection options")
    group.add_argument("-b", "--butler-config", dest="butler_config", default=None, metavar="PATH",
                       help="Location of the gen3 butler/registry config file.")
    group.add_argument("-i", "--input", dest="input", action=_InputCollectionAction,
                       metavar="COLL,DSTYPE:COLL", default={},
                       help=("Comma-separated names of the data butler collection. "
                             "If collection includes dataset type name separated by colon "
                             "then collection is only used for that specific dataset type. "
                             "Pre-flight uses these collections to search for input datasets. "
                             "Task execution stage only uses first global collection name "
                             "to override collection specified in Butler configuration file."))
    group.add_argument("-o", "--output", dest="output", type=_outputCollectionType,
                       metavar="COLL,DSTYPE:COLL", default={},
                       help=("Comma-separated names of the data butler collection. "
                             "See description of --input option.  This option only allows "
                             "single collection (per-dataset type or global)."))
    group.add_argument("-d", "--data-query", dest="data_query", default="", metavar="QUERY",
                       help="User data selection expression.")


def _makeMetaOutputOptions(parser):
    """Add a set of options describing output metadata.

    Parameters
    ----------
    parser : `argparse.ArgumentParser`
    """
    group = parser.add_argument_group("Meta-information output options")
    group.add_argument("--skip-init-writes", dest="skip_init_writes", default=False,
                       action="store_true",
                       help="Do not write collection-wide 'init output' datasets (e.g. schemas).")
    group.add_argument("--init-only", dest="init_only", default=False,
                       action="store_true",
                       help=("Do not actually run; just register dataset types and/or save init outputs."))
    group.add_argument("--register-dataset-types", dest="register_dataset_types", default=False,
                       action="store_true",
                       help="Register DatasetTypes that do not already exist in the Registry.")
    group.add_argument("--clobber-config", action="store_true", dest="clobberConfig", default=False,
                       help=("backup and then overwrite existing config files instead of checking them "
                             "(safe with -j, but not all other forms of parallel execution)"))
    group.add_argument("--no-backup-config", action="store_true", dest="noBackupConfig", default=False,
                       help="Don't copy config to file~N backup.")
    group.add_argument("--clobber-versions", action="store_true", dest="clobberVersions", default=False,
                       help=("backup and then overwrite existing package versions instead of checking"
                             "them (safe with -j, but not all other forms of parallel execution)"))
    group.add_argument("--no-versions", action="store_true", dest="noVersions", default=False,
                       help="don't check package versions; useful for development")


def _makeLoggingOptions(parser):
    """Add a set of options for logging configuration.

    Parameters
    ----------
    parser : `argparse.ArgumentParser`
    """
    group = parser.add_argument_group("Logging options")
    group.add_argument("-L", "--loglevel", action=_LogLevelAction, default=[],
                       help="logging level; supported levels are [trace|debug|info|warn|error|fatal]",
                       metavar="LEVEL|COMPONENT=LEVEL")
    group.add_argument("--longlog", action="store_true", help="use a more verbose format for the logging")
    group.add_argument("--debug", action="store_true", dest="enableLsstDebug",
                       help="enable debugging output using lsstDebug facility (imports debug.py)")


def _makePipelineOptions(parser):
    """Add a set of options for building a pipeline.

    Parameters
    ----------
    parser : `argparse.ArgumentParser`
    """
    group = parser.add_argument_group("Pipeline building options")
    group.add_argument("-p", "--pipeline", dest="pipeline",
                       help="Location of a serialized pipeline definition (pickle file).",
                       metavar="PATH")
    group.add_argument("-t", "--task", metavar="TASK[:LABEL]",
                       dest="pipeline_actions", action='append', type=_ACTION_ADD_TASK,
                       help="Task name to add to pipeline, must be a fully qualified task name. "
                       "Task name can be followed by colon and "
                       "label name, if label is not given than task base name (class name) "
                       "is used as label.")
    group.add_argument("--delete", metavar="LABEL",
                       dest="pipeline_actions", action='append', type=_ACTION_DELETE_TASK,
                       help="Delete task with given label from pipeline.")
    group.add_argument("-c", "--config", metavar="LABEL:NAME=VALUE",
                       dest="pipeline_actions", action='append', type=_ACTION_CONFIG,
                       help="Configuration override(s) for a task with specified label, "
                       "e.g. -c task:foo=newfoo -c task:bar.baz=3.")
    group.add_argument("-C", "--configfile", metavar="LABEL:PATH",
                       dest="pipeline_actions", action='append', type=_ACTION_CONFIG_FILE,
                       help="Configuration override file(s), applies to a task with a given label.")
    group.add_argument("--order-pipeline", dest="order_pipeline",
                       default=False, action="store_true",
                       help="Order tasks in pipeline based on their data dependencies, "
                       "ordering is performed as last step before saving or executing "
                       "pipeline.")
    group.add_argument("-s", "--save-pipeline", dest="save_pipeline",
                       help="Location for storing a serialized pipeline definition (pickle file).",
                       metavar="PATH")
    group.add_argument("--pipeline-dot", dest="pipeline_dot",
                       help="Location for storing GraphViz DOT representation of a pipeline.",
                       metavar="PATH")
    group.add_argument("--instrument", metavar="instrument",
                       dest="pipeline_actions", action="append", type=_ACTION_ADD_INSTRUMENT,
                       help="Add an instrument which will be used to load config overrides when"
                            " defining a pipeline. This must be the fully qualified class name")


def _makeQuntumGraphOptions(parser):
    """Add a set of options controlling quantum graph generation.

    Parameters
    ----------
    parser : `argparse.ArgumentParser`
    """
    group = parser.add_argument_group("Quantum graph building options")
    group.add_argument("-g", "--qgraph", dest="qgraph",
                       help="Location for a serialized quantum graph definition "
                       "(pickle file). If this option is given then all input data "
                       "options and pipeline-building options cannot be used.",
                       metavar="PATH")
    groupex = group.add_mutually_exclusive_group()
    groupex.add_argument("--skip-existing", dest="skip_existing",
                         default=False, action="store_true",
                         help="If all Quantum outputs already exist in output collection "
                         "then Quantum will be excluded from QuantumGraph.")
    groupex.add_argument("--clobber-output", dest="clobber_output",
                         default=False, action="store_true",
                         help="Ignore or replace existing output datasets in output collecton. "
                         "With this option existing output datasets are ignored when generating "
                         "QuantumGraph, and they are removed from a collection prior to "
                         "executing individual Quanta. This option is exclusive with "
                         "--skip-existing option.")
    group.add_argument("-q", "--save-qgraph", dest="save_qgraph",
                       help="Location for storing a serialized quantum graph definition "
                       "(pickle file).",
                       metavar="PATH")
    group.add_argument("--save-single-quanta", dest="save_single_quanta",
                       help="Format string of locations for storing individual quantum graph "
                       "definition (pickle files). The curly brace {} in the input string "
                       "will be replaced by a quantum number.",
                       metavar="PATH")
    group.add_argument("--qgraph-dot", dest="qgraph_dot",
                       help="Location for storing GraphViz DOT representation of a "
                       "quantum graph.",
                       metavar="PATH")


def _makeExecOptions(parser):
    """Add options controlling how tasks are executed.

    Parameters
    ----------
    parser : `argparse.ArgumentParser`
    """
    group = parser.add_argument_group("Execution options")
    group.add_argument("--doraise", action="store_true",
                       help="raise an exception on error (else log a message and continue)?")
    group.add_argument("--profile", metavar="PATH", help="Dump cProfile statistics to filename")

    # parallelism options
    group.add_argument("-j", "--processes", type=int, default=1, help="Number of processes to use")
    group.add_argument("--timeout", type=float,
                       help="Timeout for multiprocessing; maximum wall time (sec)")

# ------------------------
#  Exported definitions --
# ------------------------


def makeParser(fromfile_prefix_chars='@', parser_class=ArgumentParser, **kwargs):
    """Make instance of command line parser for `CmdLineFwk`.

    Creates instance of parser populated with all options that are supported
    by command line activator. There is no additional logic in this class,
    all semantics is handled by the activator class.

    Parameters
    ----------
    fromfile_prefix_chars : `str`, optional
        Prefix for arguments to be used as options files (default: `@`)
    parser_class : `type`, optional
        Specifies the class of the argument parser, by default
        `ArgumentParser` is used.
    kwargs : extra keyword arguments
        Passed directly to `parser_class` constructor

    Returns
    -------
    instance of `parser_class`
    """

    parser = parser_class(usage="%(prog)s subcommand [options]",
                          fromfile_prefix_chars=fromfile_prefix_chars,
                          epilog=_EPILOG,
                          formatter_class=RawDescriptionHelpFormatter,
                          **kwargs)

    # define sub-commands
    subparsers = parser.add_subparsers(dest="subcommand",
                                       title="commands",
                                       description=("Valid commands, use `<command> --help' to get "
                                                    "more info about each command:"),
                                       prog=parser.prog)
    # Python3 workaround, see http://bugs.python.org/issue9253#msg186387
    # The issue was fixed in Python 3.6, workaround is not need starting with that version
    subparsers.required = True

    for subcommand in ("build", "qgraph", "run"):
        # show/run sub-commands, they are all identical except for the
        # command itself and description

        if subcommand == "build":
            description = textwrap.dedent("""\
                Build and optionally save pipeline definition.
                This does not require input data to be specified.""")
        elif subcommand == "qgraph":
            description = textwrap.dedent("""\
                Build and optionally save pipeline and quantum graph.""")
        else:
            description = textwrap.dedent("""\
                Build and execute pipeline and quantum graph.""")

        subparser = subparsers.add_parser(subcommand,
                                          description=description,
                                          epilog=_EPILOG,
                                          formatter_class=RawDescriptionHelpFormatter)
        subparser.set_defaults(subparser=subparser,
                               pipeline_actions=[])
        _makeLoggingOptions(subparser)
        _makePipelineOptions(subparser)

        if subcommand in ("qgraph", "run"):
            _makeQuntumGraphOptions(subparser)
            _makeButlerOptions(subparser)

        if subcommand == "run":
            _makeExecOptions(subparser)
            _makeMetaOutputOptions(subparser)

        subparser.add_argument("--show", metavar="ITEM|ITEM=VALUE", action="append", default=[],
                               help="Dump various info to standard output. Possible items are: "
                               "`config', `config=[Task::]<PATTERN>' or "
                               "`config=[Task::]<PATTERN>:NOIGNORECASE' to dump configuration "
                               "fields possibly matching given pattern and/or task label; "
                               "`history=<FIELD>' to dump configuration history for a field, "
                               "field name is specified as [Task::][SubTask.]Field; "
                               "`dump-config', `dump-config=Task' to dump complete configuration "
                               "for a task given its label or all tasks; "
                               "`pipeline' to show pipeline composition; "
                               "`graph' to show information about quanta; "
                               "`workflow' to show information about quanta and their dependency; "
                               "`tasks' to show task composition.")

    return parser
