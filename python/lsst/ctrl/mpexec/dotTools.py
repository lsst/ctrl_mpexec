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

"""Module defining few methods to generate GraphViz diagrams from pipelines
or quantum graphs.
"""

from __future__ import annotations

__all__ = ["graph2dot", "pipeline2dot"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import io
import re
from typing import TYPE_CHECKING, Any, Iterable, Union

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import DatasetType, DimensionUniverse
from lsst.pipe.base import Pipeline, connectionTypes, iterConnections

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef
    from lsst.pipe.base import QuantumGraph, QuantumNode, TaskDef

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# Node styles indexed by node type.
_STYLES = dict(
    task=dict(shape="box", style="filled,bold", fillcolor="gray70"),
    quantum=dict(shape="box", style="filled,bold", fillcolor="gray70"),
    dsType=dict(shape="box", style="rounded,filled", fillcolor="gray90"),
    dataset=dict(shape="box", style="rounded,filled", fillcolor="gray90"),
)


def _renderNode(file: io.TextIOBase, nodeName: str, style: str, labels: list[str]) -> None:
    """Render GV node"""
    label = r"\n".join(labels)
    attrib_dict = dict(_STYLES[style], label=label)
    attrib = ", ".join([f'{key}="{val}"' for key, val in attrib_dict.items()])
    print(f'"{nodeName}" [{attrib}];', file=file)


def _renderTaskNode(nodeName: str, taskDef: TaskDef, file: io.TextIOBase, idx: Any = None) -> None:
    """Render GV node for a task"""
    labels = [taskDef.label, taskDef.taskName]
    if idx is not None:
        labels.append(f"index: {idx}")
    if taskDef.connections:
        # don't print collection of str directly to avoid visually noisy quotes
        dimensions_str = ", ".join(sorted(taskDef.connections.dimensions))
        labels.append(f"dimensions: {dimensions_str}")
    _renderNode(file, nodeName, "task", labels)


def _renderQuantumNode(
    nodeName: str, taskDef: TaskDef, quantumNode: QuantumNode, file: io.TextIOBase
) -> None:
    """Render GV node for a quantum"""
    labels = [f"{quantumNode.nodeId}", taskDef.label]
    dataId = quantumNode.quantum.dataId
    assert dataId is not None, "Quantum DataId cannot be None"
    labels.extend(f"{key} = {dataId[key]}" for key in sorted(dataId.keys()))
    _renderNode(file, nodeName, "quantum", labels)


def _renderDSTypeNode(name: str, dimensions: list[str], file: io.TextIOBase) -> None:
    """Render GV node for a dataset type"""
    labels = [name]
    if dimensions:
        labels.append("Dimensions: " + ", ".join(sorted(dimensions)))
    _renderNode(file, name, "dsType", labels)


def _renderDSNode(nodeName: str, dsRef: DatasetRef, file: io.TextIOBase) -> None:
    """Render GV node for a dataset"""
    labels = [dsRef.datasetType.name, f"run: {dsRef.run!r}"]
    labels.extend(f"{key} = {dsRef.dataId[key]}" for key in sorted(dsRef.dataId.keys()))
    _renderNode(file, nodeName, "dataset", labels)


def _renderEdge(fromName: str, toName: str, file: io.TextIOBase, **kwargs: Any) -> None:
    """Render GV edge"""
    if kwargs:
        attrib = ", ".join([f'{key}="{val}"' for key, val in kwargs.items()])
        print(f'"{fromName}" -> "{toName}" [{attrib}];', file=file)
    else:
        print(f'"{fromName}" -> "{toName}";', file=file)


def _datasetRefId(dsRef: DatasetRef) -> str:
    """Make an identifying string for given ref"""
    dsId = [dsRef.datasetType.name]
    dsId.extend(f"{key} = {dsRef.dataId[key]}" for key in sorted(dsRef.dataId.keys()))
    return ":".join(dsId)


def _makeDSNode(dsRef: DatasetRef, allDatasetRefs: dict[str, str], file: io.TextIOBase) -> str:
    """Make new node for dataset if  it does not exist.

    Returns node name.
    """
    dsRefId = _datasetRefId(dsRef)
    nodeName = allDatasetRefs.get(dsRefId)
    if nodeName is None:
        idx = len(allDatasetRefs)
        nodeName = "dsref_{}".format(idx)
        allDatasetRefs[dsRefId] = nodeName
        _renderDSNode(nodeName, dsRef, file)
    return nodeName


# ------------------------
#  Exported definitions --
# ------------------------


def graph2dot(qgraph: QuantumGraph, file: Any) -> None:
    """Convert QuantumGraph into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.

    Parameters
    ----------
    qgraph: `pipe.base.QuantumGraph`
        QuantumGraph instance.
    file : str or file object
        File where GraphViz graph (DOT language) is written, can be a file name
        or file object.

    Raises
    ------
    `OSError` is raised when output file cannot be open.
    `ImportError` is raised when task class cannot be imported.
    """
    # open a file if needed
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    print("digraph QuantumGraph {", file=file)

    allDatasetRefs: dict[str, str] = {}
    for taskId, taskDef in enumerate(qgraph.taskGraph):
        quanta = qgraph.getNodesForTask(taskDef)
        for qId, quantumNode in enumerate(quanta):
            # node for a task
            taskNodeName = "task_{}_{}".format(taskId, qId)
            _renderQuantumNode(taskNodeName, taskDef, quantumNode, file)

            # quantum inputs
            for dsRefs in quantumNode.quantum.inputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    _renderEdge(nodeName, taskNodeName, file)

            # quantum outputs
            for dsRefs in quantumNode.quantum.outputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    _renderEdge(taskNodeName, nodeName, file)

    print("}", file=file)
    if close:
        file.close()


def pipeline2dot(pipeline: Union[Pipeline, Iterable[TaskDef]], file: Any) -> None:
    """Convert Pipeline into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.
    Unlike other methods this method does not validate graph consistency.

    Parameters
    ----------
    pipeline : `pipe.base.Pipeline`
        Pipeline description.
    file : str or file object
        File where GraphViz graph (DOT language) is written, can be a file name
        or file object.

    Raises
    ------
    `OSError` is raised when output file cannot be open.
    `ImportError` is raised when task class cannot be imported.
    `MissingTaskFactoryError` is raised when TaskFactory is needed but not
    provided.
    """
    universe = DimensionUniverse()

    def expand_dimensions(connection: connectionTypes.BaseConnection) -> list[str]:
        """Returns expanded list of dimensions, with special skypix treatment.

        Parameters
        ----------
        dimensions : `list` [`str`]

        Returns
        -------
        dimensions : `list` [`str`]
        """
        dimension_set = set()
        if isinstance(connection, connectionTypes.DimensionedConnection):
            dimension_set = set(connection.dimensions)
        skypix_dim = []
        if "skypix" in dimension_set:
            dimension_set.remove("skypix")
            skypix_dim = ["skypix"]
        dimension_graph = universe.extract(dimension_set)
        return list(dimension_graph.names) + skypix_dim

    # open a file if needed
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    print("digraph Pipeline {", file=file)

    allDatasets: set[Union[str, tuple[str, str]]] = set()
    if isinstance(pipeline, Pipeline):
        pipeline = pipeline.toExpandedPipeline()

    # The next two lines are a workaround until DM-29658 at which time metadata
    # connections should start working with the above code
    labelToTaskName = {}
    metadataNodesToLink = set()

    for idx, taskDef in enumerate(sorted(pipeline, key=lambda x: x.label)):
        # node for a task
        taskNodeName = "task{}".format(idx)

        # next line is workaround until DM-29658
        labelToTaskName[taskDef.label] = taskNodeName

        _renderTaskNode(taskNodeName, taskDef, file, None)

        metadataRePattern = re.compile("^(.*)_metadata$")
        for attr in sorted(iterConnections(taskDef.connections, "inputs"), key=lambda x: x.name):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            nodeName, component = DatasetType.splitDatasetTypeName(attr.name)
            _renderEdge(attr.name, taskNodeName, file)
            # connect component dataset types to the composite type that
            # produced it
            if component is not None and (nodeName, attr.name) not in allDatasets:
                _renderEdge(nodeName, attr.name, file)
                allDatasets.add((nodeName, attr.name))
                if nodeName not in allDatasets:
                    dimensions = expand_dimensions(attr)
                    _renderDSTypeNode(nodeName, dimensions, file)
            # The next if block is a workaround until DM-29658 at which time
            # metadata connections should start working with the above code
            if (match := metadataRePattern.match(attr.name)) is not None:
                matchTaskLabel = match.group(1)
                metadataNodesToLink.add((matchTaskLabel, attr.name))

        for attr in sorted(iterConnections(taskDef.connections, "prerequisiteInputs"), key=lambda x: x.name):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            # use dashed line for prerequisite edges to distinguish them
            _renderEdge(attr.name, taskNodeName, file, style="dashed")

        for attr in sorted(iterConnections(taskDef.connections, "outputs"), key=lambda x: x.name):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            _renderEdge(taskNodeName, attr.name, file)

    # This for loop is a workaround until DM-29658 at which time metadata
    # connections should start working with the above code
    for matchLabel, dsTypeName in metadataNodesToLink:
        # only render an edge to metadata if the label is part of the current
        # graph
        if (result := labelToTaskName.get(matchLabel)) is not None:
            _renderEdge(result, dsTypeName, file)

    print("}", file=file)
    if close:
        file.close()
