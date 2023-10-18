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


import itertools
from typing import Any

from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.registry import MissingCollectionError

from .confirmable import ConfirmableResult

advice = (
    'Use "butler collection-chain --mode remove" to remove this collection from its parent or\n'
    'use "butler remove-collections" to remove that parent entirely.'
)


class ChildHasMultipleParentsFailure:
    """Failure when the child has multiple parents."""

    def __init__(self, child: str, parents: list[str]):
        self.child = child
        self.parents = parents

    def __str__(self) -> str:
        parents = ", ".join([f'"{p}"' for p in self.parents])
        return f'Collection "{self.child}" is in multiple chained collections: {parents}.\n {advice}'


class TopCollectionHasParentsFailure:
    """Failure when the top collection has parents."""

    def __init__(self, collection: str, parents: list[str]):
        self.collection = collection
        self.parents = parents

    def __str__(self) -> str:
        parents = ", ".join([f'"{p}"' for p in self.parents])
        return (
            f'The passed-in collection "{self.collection}" must not be contained in other collections but '
            f"is contained in collection(s) {parents}.\n {advice}"
        )


class TopCollectionIsNotChainedFailure:
    """Failure when the top collection is not a chain."""

    def __init__(self, collection: str, collection_type: CollectionType):
        self.collection = collection
        self.collection_type = collection_type

    def __str__(self) -> str:
        return (
            "The passed-in collection must be a CHAINED collection; "
            f'"{self.collection}" is a {self.collection_type.name} collection.'
        )


class TopCollectionNotFoundFailure:
    """Failure when the top collection is not found."""

    def __init__(self, collection: str):
        self.collection = collection

    def __str__(self) -> str:
        return f'The passed-in collection "{self.collection}" was not found.'


class PurgeResult(ConfirmableResult):
    """The results of the purge command."""

    def __init__(self, butler_config: str):
        self.runs_to_remove: list[str] = []
        self.chains_to_remove: list[str] = []
        self.others_to_remove: list[str] = []
        self.butler_config = butler_config
        self.failure: Any = None

    @property
    def describe_failure(self) -> str:
        return str(self.failure)

    def describe(self, will: bool) -> str:
        msg = ""
        if will:
            msg += "Will remove:\n"
        else:
            msg += "Removed:\n"
        msg += f"  runs: {', '.join(self.runs_to_remove)}\n"
        msg += f"  chains: {', '.join(self.chains_to_remove)}\n"
        msg += f"  others: {', '.join(self.others_to_remove)}"
        return msg

    def on_confirmation(self) -> None:
        if self.failure:
            # This should not happen, it is a logic error.
            raise RuntimeError("Can not purge, there were errors preparing collections.")
        butler = Butler.from_config(self.butler_config, writeable=True)
        with butler.transaction():
            for c in itertools.chain(self.others_to_remove, self.chains_to_remove):
                butler.registry.removeCollection(c)
            butler.removeRuns(self.runs_to_remove)

    @property
    def failed(self) -> bool:
        return bool(self.failure)

    @property
    def can_continue(self) -> bool:
        # Will always be true: at the very least there is a top level CHAINED
        # collection to remove. And if the top level collection is not found it
        # results in a TopCollectionNotFoundFailure.
        return True

    def fail(
        self,
        failure: (
            ChildHasMultipleParentsFailure
            | TopCollectionHasParentsFailure
            | TopCollectionIsNotChainedFailure
            | TopCollectionNotFoundFailure
        ),
    ) -> None:
        self.failure = failure


def check_parents(butler: Butler, child: str, expected_parents: list[str]) -> list[str] | None:
    """Check that the parents of a child collection match the provided
    expected parents.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        The butler to the current repo.
    child : `str`
        The child collection to check.
    expected_parents : `list` [`str`]
        The list of expected parents.

    Returns
    -------
    parents: `list` or `None`
        If `None` then the child's parents matched the expected parents. If
        not `None`, then the actual parents of the child.
    """
    parents = butler.registry.getCollectionParentChains(child)
    if parents != set(expected_parents):
        return list(parents)
    return None


def prepare_to_remove(
    top_collection: str,
    parent_collection: str,
    butler: Butler,
    recursive: bool,
    purge_result: PurgeResult,
) -> None:
    """Add a CHAINED colleciton to the list of chains to remove and then
    find its children and add them to the appropriate lists for removal.

    Verify that the children of the CHAINED collection have exactly one
    parent (that CHAINED collection). If `recursive` is `True` then run
    recursively on the children of any child CHAINED collections.

    Parameters
    ----------
    top_collection : `str`
        The name of the top CHAINED collection being purged.
        Child collections to remove must start with this name,
        other child collections will be ignored.
    parent_collection : `str`
        The parent CHAINED collection currently being removed.
    butler : `~lsst.daf.butler.Butler`
        The butler to the repo.
    recursive : `bool`
        If True then children of the top collection that are also CHAINED
        collections will be purged.
    purge_result : `PurgeResult`
        The data structure being populated with failure information or
        collections to remove.
    """
    assert butler.registry.getCollectionType(parent_collection) == CollectionType.CHAINED
    purge_result.chains_to_remove.append(parent_collection)
    for child in butler.registry.getCollectionChain(parent_collection):
        if child.startswith(top_collection):
            if parents := check_parents(butler, child, [parent_collection]):
                purge_result.fail(ChildHasMultipleParentsFailure(child, parents))
            collection_type = butler.registry.getCollectionType(child)
            if collection_type == CollectionType.RUN:
                purge_result.runs_to_remove.append(child)
            elif collection_type == CollectionType.CHAINED:
                if recursive:
                    prepare_to_remove(
                        top_collection=top_collection,
                        parent_collection=child,
                        butler=butler,
                        recursive=recursive,
                        purge_result=purge_result,
                    )
                else:
                    purge_result.chains_to_remove.append(child)
            else:
                purge_result.others_to_remove.append(child)


def purge(
    butler_config: str,
    collection: str,
    recursive: bool,
) -> PurgeResult:
    """Purge a CHAINED collection and it's children from a repository.

    Parameters
    ----------
    butler_config : `str`
        The path location of the gen3 butler/registry config file.
    collection : `str`
        The name of the CHAINED colleciton to purge.
    recursive : bool
        If True then children of the top collection that are also CHAINED
        collections will be purged.

    Returns
    -------
    purge_result : `PurgeResult`
        The description of what datasets to remove and/or failures encountered
        while preparing to remove datasets to remove, and a completion function
        to remove the datasets after confirmation, if needed.
    """
    result = PurgeResult(butler_config)
    butler = Butler.from_config(butler_config)

    try:
        collection_type = butler.registry.getCollectionType(collection)
    except MissingCollectionError:
        result.fail(TopCollectionNotFoundFailure(collection))
        return result

    if collection_type != CollectionType.CHAINED:
        result.fail(TopCollectionIsNotChainedFailure(collection, collection_type))
    elif parents := check_parents(butler, collection, []):
        result.fail(TopCollectionHasParentsFailure(collection, parents))
    else:
        prepare_to_remove(
            top_collection=collection,
            parent_collection=collection,
            purge_result=result,
            butler=butler,
            recursive=recursive,
        )
    return result
