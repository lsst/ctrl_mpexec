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

__all__ = [
    "ButlerFactory",
    "OutputChainedCollectionInfo",
    "OutputRunCollectionInfo",
]

import atexit
import shutil
from collections.abc import Iterable, Sequence

from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.datastore.cache_manager import DatastoreCacheManager
from lsst.daf.butler.registry import MissingCollectionError, RegistryDefaults
from lsst.daf.butler.registry.wildcards import CollectionWildcard
from lsst.pipe.base import Instrument, PipelineGraph
from lsst.pipe.base.pipeline_graph import NodeType
from lsst.resources import ResourcePathExpression
from lsst.utils.logging import getLogger

_LOG = getLogger(__name__)


class OutputChainedCollectionInfo:
    """A helper class for handling command-line arguments related to an output
    `~lsst.daf.butler.CollectionType.CHAINED` collection.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler that collections will be added to and/or queried from.
    name : `str`
        Name of the collection given on the command line.
    """

    def __init__(self, butler: Butler, name: str):
        self.name = name
        try:
            self.chain = tuple(butler.collections.get_info(name).children)
            self.exists = True
        except MissingCollectionError:
            self.chain = ()
            self.exists = False

    def __str__(self) -> str:
        return self.name

    name: str
    """Name of the collection provided on the command line (`str`).
    """

    exists: bool
    """Whether this collection already exists in the butler (`bool`).
    """

    chain: tuple[str, ...]
    """The definition of the collection, if it already exists (`tuple`[`str`]).

    Empty if the collection does not already exist.
    """


class OutputRunCollectionInfo:
    """A helper class for handling command-line arguments related to an output
    `~lsst.daf.butler.CollectionType.RUN` collection.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler that collections will be added to and/or queried from.
    name : `str`
        Name of the collection given on the command line.
    """

    def __init__(self, butler: Butler, name: str):
        self.name = name
        try:
            actual_type = butler.collections.get_info(name).type
            if actual_type is not CollectionType.RUN:
                raise TypeError(f"Collection '{name}' exists but has type {actual_type.name}, not RUN.")
            self.exists = True
        except MissingCollectionError:
            self.exists = False

    name: str
    """Name of the collection provided on the command line (`str`).
    """

    exists: bool
    """Whether this collection already exists in the butler (`bool`).
    """


class ButlerFactory:
    """A helper class for processing command-line arguments related to input
    and output collections.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler that collections will be added to and/or queried from.
    output : `str` or `None`
        The name of a `~lsst.daf.butler.CollectionType.CHAINED` input/output
        collection.
    output_run : `str` or `None`
        The name of a `~lsst.daf.butler.CollectionType.RUN` input/output
        collection.
    inputs : `str` or `~collections.abc.Iterable` [`str`]
        Input collection name or iterable of collection names.
    extend_run : `bool`
        A boolean indicating whether ``output_run`` should already exist and be
        extended.
    rebase : `bool`
        A boolean indicating whether to force the ``output`` collection to be
        consistent with ``inputs`` and ``output`` run such that the ``output``
        collection has output run collections first (i.e. those that start with
        the same prefix), then the new inputs, then any original inputs not
        included in the new inputs.
    writeable : `bool`
        If `True`, a `~lsst.daf.butler.Butler` is being initialized in a
        context where actual writes should happens, and hence no output run
        is necessary.

    Raises
    ------
    ValueError
        Raised if ``writeable is True`` but there are no output collections.
    """

    def __init__(
        self,
        butler: Butler,
        *,
        output: str | None,
        output_run: str | None,
        inputs: str | Iterable[str],
        extend_run: bool = False,
        rebase: bool = False,
        writeable: bool,
    ):
        if output is not None:
            self.output = OutputChainedCollectionInfo(butler, output)
        else:
            self.output = None
        if output_run is not None:
            if rebase and self.output and not output_run.startswith(self.output.name):
                raise ValueError("Cannot rebase if output run does not start with output collection name.")
            self.output_run = OutputRunCollectionInfo(butler, output_run)
        elif self.output is not None:
            if extend_run:
                if not self.output.chain:
                    raise ValueError("Cannot use --extend-run option with non-existing or empty output chain")
                run_name = self.output.chain[0]
            else:
                run_name = f"{self.output}/{Instrument.makeCollectionTimestamp()}"
            self.output_run = OutputRunCollectionInfo(butler, run_name)
        elif not writeable:
            # If we're not writing yet, ok to have no output run.
            self.output_run = None
        else:
            raise ValueError("Cannot write without at least one of (--output, --output-run).")
        # Recursively flatten any input CHAINED collections.  We do this up
        # front so we can tell if the user passes the same inputs on subsequent
        # calls, even though we also flatten when we define the output CHAINED
        # collection.
        self.inputs = tuple(butler.collections.query(inputs, flatten_chains=True)) if inputs else ()

        # If things are inconsistent and user has asked for a rebase then
        # construct the new output chain.
        if rebase and self._check_output_input_consistency():
            assert self.output is not None
            newOutputChain = [item for item in self.output.chain if item.startswith(self.output.name)]
            newOutputChain.extend([item for item in self.inputs if item not in newOutputChain])
            newOutputChain.extend([item for item in self.output.chain if item not in newOutputChain])
            self.output.chain = tuple(newOutputChain)

    def check(self, *, extend_run: bool, replace_run: bool, prune_replaced: str | None = None) -> None:
        """Check command-line options for consistency with each other and the
        data repository.

        Parameters
        ----------
        extend_run : `bool`
            Whether the ``output_run`` should already exist and be extended.
        replace_run : `bool`
            Whether the ``output_run`` should be replaced in the ``output``
            chain.
        prune_replaced : `str` or `None`
            If ``replace_run=True``, whether/how datasets in the old run should
            be removed.  Options are ``"purge"``, ``"unstore"``, and `None`.
        """
        assert not (extend_run and replace_run), "In mutually-exclusive group in ArgumentParser."
        if consistencyError := self._check_output_input_consistency():
            raise ValueError(consistencyError)

        if extend_run:
            if self.output_run is None:
                raise ValueError("Cannot --extend-run when no output collection is given.")
            elif not self.output_run.exists:
                raise ValueError(
                    f"Cannot --extend-run; output collection '{self.output_run.name}' does not exist."
                )
        if not extend_run and self.output_run is not None and self.output_run.exists:
            raise ValueError(
                f"Output run '{self.output_run.name}' already exists, but --extend-run was not given."
            )
        if prune_replaced and not replace_run:
            raise ValueError("--prune-replaced requires --replace-run.")
        if replace_run and (self.output is None or not self.output.exists):
            raise ValueError("--output must point to an existing CHAINED collection for --replace-run.")

    def _check_output_input_consistency(self) -> str | None:
        if self.inputs and self.output is not None and self.output.exists:
            # Passing the same inputs that were used to initialize the output
            # collection is allowed; this means the inputs must appear as a
            # contiguous subsequence of outputs (normally they're also at the
            # end, but --rebase will in general put them in the middle).
            for n in reversed(range(1 + len(self.output.chain) - len(self.inputs))):
                if self.inputs == self.output.chain[n : n + len(self.inputs)]:
                    return None
            return (
                f"Output CHAINED collection {self.output.name!r} exists and does not include the "
                f"same sequence of (flattened) input collections {self.inputs} as a contiguous "
                "subsequence. "
                "Use --rebase to ignore this problem and reset the output collection, but note that "
                "this may obfuscate what inputs were actually used to produce these outputs."
            )
        return None

    @classmethod
    def _make_read_parts(
        cls,
        butler_config: ResourcePathExpression,
        *,
        output: str | None,
        output_run: str | None,
        inputs: str | Iterable[str],
        extend_run: bool = False,
        rebase: bool = False,
        replace_run: bool,
        prune_replaced: str | None = None,
    ) -> tuple[Butler, Sequence[str], ButlerFactory]:
        """Parse arguments to support implementations of `make_read_butler` and
        `make_butler_and_collections`.

        Parameters
        ----------
        butler_config : convertible to `lsst.resources.ResourcePath`
            Path to configuration for the butler.
        output : `str` or `None`
            The name of a `~lsst.daf.butler.CollectionType.CHAINED`
            input/output collection.
        output_run : `str` or `None`
            The name of a `~lsst.daf.butler.CollectionType.RUN` input/output
            collection.
        inputs : `str` or `~collections.abc.Iterable` [`str`]
            Input collection name or iterable of collection names.
        extend_run : `bool`
            A boolean indicating whether ``output_run`` should already exist
            and be extended.
        rebase : `bool`
            A boolean indicating whether to force the ``output`` collection to
            be consistent with ``inputs`` and ``output`` run such that the
            ``output`` collection has output run collections first (i.e. those
            that start with the same prefix), then the new inputs, then any
            original inputs not included in the new inputs.
        replace_run : `bool`
            Whether the ``output_run`` should be replaced in the ``output``
            chain.
        prune_replaced : `str` or `None`
            If ``replace_run=True``, whether/how datasets in the old run should
            be removed.  Options are ``"purge"``, ``"unstore"``, and `None`.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-only butler constructed from the repo at
            ``args.butler_config``, but with no default collections.
        inputs : `~collections.abc.Sequence` [ `str` ]
            A collection search path constructed according to ``args``.
        self : `ButlerFactory`
            A new `ButlerFactory` instance representing the processed version
            of ``args``.
        """
        butler = Butler.from_config(butler_config, writeable=False)
        try:
            self = cls(
                butler,
                output=output,
                output_run=output_run,
                inputs=inputs,
                extend_run=extend_run,
                rebase=rebase,
                writeable=False,
            )
            self.check(extend_run=extend_run, replace_run=replace_run, prune_replaced=prune_replaced)
            if self.output and self.output.exists:
                if replace_run:
                    replaced = self.output.chain[0]
                    inputs = list(self.output.chain[1:])
                    _LOG.debug(
                        "Simulating collection search in '%s' after removing '%s'.",
                        self.output.name,
                        replaced,
                    )
                else:
                    inputs = [self.output.name]
            else:
                inputs = list(self.inputs)
            if extend_run:
                assert self.output_run is not None, "Output collection has to be specified."
                inputs.insert(0, self.output_run.name)
            collSearch = CollectionWildcard.from_expression(inputs).require_ordered()
        except Exception:
            butler.close()
            raise
        return butler, collSearch, self

    @classmethod
    def make_butler_and_collections(
        cls,
        butler_config: ResourcePathExpression,
        *,
        output: str | None,
        output_run: str | None,
        inputs: str | Iterable[str],
        extend_run: bool = False,
        rebase: bool = False,
        replace_run: bool,
        prune_replaced: str | None = None,
    ) -> tuple[Butler, Sequence[str], str | None]:
        """Return a read-only butler, a collection search path, and the name
        of the run to be used for future writes.

        Parameters
        ----------
        butler_config : convertible to `lsst.resources.ResourcePath`
            Path to configuration for the butler.
        output : `str` or `None`
            The name of a `~lsst.daf.butler.CollectionType.CHAINED`
            input/output collection.
        output_run : `str` or `None`
            The name of a `~lsst.daf.butler.CollectionType.RUN` input/output
            collection.
        inputs : `str` or `~collections.abc.Iterable` [`str`]
            Input collection name or iterable of collection names.
        extend_run : `bool`
            A boolean indicating whether ``output_run`` should already exist
            and be extended.
        rebase : `bool`
            A boolean indicating whether to force the ``output`` collection to
            be consistent with ``inputs`` and ``output`` run such that the
            ``output`` collection has output run collections first (i.e. those
            that start with the same prefix), then the new inputs, then any
            original inputs not included in the new inputs.
        replace_run : `bool`
            Whether the ``output_run`` should be replaced in the ``output``
            chain.
        prune_replaced : `str` or `None`
            If ``replace_run=True``, whether/how datasets in the old run should
            be removed.  Options are ``"purge"``, ``"unstore"``, and `None`.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-only butler that collections will be added to and/or queried
            from.
        inputs : `Sequence` [ `str` ]
            Collections to search for datasets.
        run : `str` or `None`
            Name of the output `~lsst.daf.butler.CollectionType.RUN` collection
            if it already exists, or `None` if it does not.
        """
        butler, inputs, self = cls._make_read_parts(
            butler_config,
            output=output,
            output_run=output_run,
            inputs=inputs,
            extend_run=extend_run,
            rebase=rebase,
            replace_run=replace_run,
            prune_replaced=prune_replaced,
        )
        run: str | None = None
        if extend_run:
            assert self.output_run is not None, "Output collection has to be specified."
        if self.output_run is not None:
            run = self.output_run.name
        _LOG.debug("Preparing butler to read from %s and expect future writes to '%s'.", inputs, run)
        return butler, inputs, run

    @staticmethod
    def define_datastore_cache() -> None:
        """Define where datastore cache directories should be found.

        Notes
        -----
        All the jobs should share a datastore cache if applicable. This
        method asks for a shared fallback cache to be defined and then
        configures an exit handler to clean it up.
        """
        defined, cache_dir = DatastoreCacheManager.set_fallback_cache_directory_if_unset()
        if defined:
            atexit.register(shutil.rmtree, cache_dir, ignore_errors=True)
            _LOG.debug("Defining shared datastore cache directory to %s", cache_dir)

    @classmethod
    def make_write_butler(
        cls,
        butler_config: ResourcePathExpression,
        pipeline_graph: PipelineGraph,
        *,
        output: str | None,
        output_run: str | None,
        inputs: str | Iterable[str],
        extend_run: bool = False,
        rebase: bool = False,
        replace_run: bool,
        prune_replaced: str | None = None,
    ) -> Butler:
        """Return a read-write butler initialized to write to and read from
        the collections specified by the given command-line arguments.

        Parameters
        ----------
        butler_config : convertible to `lsst.resources.ResourcePath`
            Path to configuration for the butler.
        pipeline_graph : `lsst.pipe.base.PipelineGraph`
            Definitions for tasks in a pipeline.
        output : `str` or `None`
            The name of a `~lsst.daf.butler.CollectionType.CHAINED`
            input/output collection.
        output_run : `str` or `None`
            The name of a `~lsst.daf.butler.CollectionType.RUN` input/output
            collection.
        inputs : `str` or `~collections.abc.Iterable` [`str`]
            Input collection name or iterable of collection names.
        extend_run : `bool`
            A boolean indicating whether ``output_run`` should already exist
            and be extended.
        rebase : `bool`
            A boolean indicating whether to force the ``output`` collection to
            be consistent with ``inputs`` and ``output`` run such that the
            ``output`` collection has output run collections first (i.e. those
            that start with the same prefix), then the new inputs, then any
            original inputs not included in the new inputs.
        replace_run : `bool`
            Whether the ``output_run`` should be replaced in the ``output``
            chain.
        prune_replaced : `str` or `None`
            If ``replace_run=True``, whether/how datasets in the old run should
            be removed.  Options are ``"purge"``, ``"unstore"``, and `None`.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A read-write butler initialized according to the given arguments.
        """
        cls.define_datastore_cache()  # Ensure that this butler can use a shared cache.
        butler = Butler.from_config(butler_config, writeable=True)
        self = cls(
            butler,
            output=output,
            output_run=output_run,
            inputs=inputs,
            extend_run=extend_run,
            rebase=rebase,
            writeable=True,
        )
        self.check(extend_run=extend_run, replace_run=replace_run, prune_replaced=prune_replaced)
        assert self.output_run is not None, "Output collection has to be specified."  # for mypy
        if self.output is not None:
            chain_definition = list(
                butler.collections.query(
                    self.output.chain if self.output.exists else self.inputs,
                    flatten_chains=True,
                    include_chains=False,
                )
            )
            if replace_run:
                replaced = chain_definition.pop(0)
                if prune_replaced == "unstore":
                    # Remove datasets from datastore
                    with butler.transaction():
                        # we want to remove regular outputs from this pipeline,
                        # but keep initOutputs, configs, and versions.
                        refs = [
                            ref
                            for ref in butler.registry.queryDatasets(..., collections=replaced)
                            if (
                                (producer := pipeline_graph.producer_of(ref.datasetType.name)) is not None
                                and producer.key.node_type is NodeType.TASK  # i.e. not TASK_INIT
                            )
                        ]
                        butler.pruneDatasets(refs, unstore=True, disassociate=False)
                elif prune_replaced == "purge":
                    # Erase entire collection and all datasets, need to remove
                    # collection from its chain collection first.
                    with butler.transaction():
                        butler.collections.redefine_chain(self.output.name, chain_definition)
                        butler.removeRuns([replaced])
                elif prune_replaced is not None:
                    raise NotImplementedError(f"Unsupported --prune-replaced option '{prune_replaced}'.")
            if not self.output.exists:
                butler.collections.register(self.output.name, CollectionType.CHAINED)
            if not extend_run:
                butler.collections.register(self.output_run.name, CollectionType.RUN)
                chain_definition.insert(0, self.output_run.name)
                butler.collections.redefine_chain(self.output.name, chain_definition)
            _LOG.debug(
                "Preparing butler to write to '%s' and read from '%s'=%s",
                self.output_run.name,
                self.output.name,
                chain_definition,
            )
            butler.registry.defaults = RegistryDefaults(
                run=self.output_run.name, collections=self.output.name
            )
        else:
            inputs = (self.output_run.name,) + self.inputs
            _LOG.debug("Preparing butler to write to '%s' and read from %s.", self.output_run.name, inputs)
            butler.registry.defaults = RegistryDefaults(run=self.output_run.name, collections=inputs)
        return butler

    output: OutputChainedCollectionInfo | None
    """Information about the output chained collection, if there is or will be
    one (`OutputChainedCollectionInfo` or `None`).
    """

    output_run: OutputRunCollectionInfo | None
    """Information about the output run collection, if there is or will be
    one (`OutputRunCollectionInfo` or `None`).
    """

    inputs: tuple[str, ...]
    """Input collections provided directly by the user (`tuple` [ `str` ]).
    """
