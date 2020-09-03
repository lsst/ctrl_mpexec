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

__all__ = ["graph2dot", "pipeline2dot"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import DimensionUniverse
from lsst.pipe.base import iterConnections, Pipeline

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# Node styles indexed by node type.
_STYLES = dict(
    task=dict(shape="box", style="filled,bold", fillcolor="gray70"),
    dsType=dict(shape="box", style="rounded,filled", fillcolor="gray90"),
    dataset=dict(shape="box", style="rounded,filled", fillcolor="gray90"),
)


def _renderNode(file, nodeName, style, labels):
    """Render GV node"""
    label = r'\n'.join(labels)
    attrib = dict(_STYLES[style], label=label)
    attrib = ", ".join([f'{key}="{val}"' for key, val in attrib.items()])
    print(f'"{nodeName}" [{attrib}];', file=file)


def _renderTaskNode(nodeName, taskDef, file, idx=None):
    """Render GV node for a task"""
    labels = [taskDef.taskName.rpartition('.')[-1]]
    if idx is not None:
        labels += [f"index: {idx}"]
    if taskDef.label:
        labels += [f"label: {taskDef.label}"]
    _renderNode(file, nodeName, "task", labels)


def _renderDSTypeNode(name, dimensions, file):
    """Render GV node for a dataset type"""
    labels = [name]
    if dimensions:
        labels += ["Dimensions: " + ", ".join(dimensions)]
    _renderNode(file, name, "dsType", labels)


def _renderDSNode(nodeName, dsRef, file):
    """Render GV node for a dataset"""
    labels = [dsRef.datasetType.name]
    labels += [f"{key} = {dsRef.dataId[key]}" for key in sorted(dsRef.dataId.keys())]
    _renderNode(file, nodeName, "dataset", labels)


def _renderEdge(fromName, toName, file, **kwargs):
    """Render GV edge"""
    if kwargs:
        attrib = ", ".join([f'{key}="{val}"' for key, val in kwargs.items()])
        print(f'"{fromName}" -> "{toName}" [{attrib}];', file=file)
    else:
        print(f'"{fromName}" -> "{toName}";', file=file)


def _datasetRefId(dsRef):
    """Make an identifying string for given ref"""
    dsId = [dsRef.datasetType.name]
    dsId += [f"{key} = {dsRef.dataId[key]}" for key in sorted(dsRef.dataId.keys())]
    return ":".join(dsId)


def _makeDSNode(dsRef, allDatasetRefs, file):
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


def graph2dot(qgraph, file):
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

    allDatasetRefs = {}
    for taskId, taskDef in enumerate(qgraph.taskGraph):

        quanta = qgraph.getQuantaForTask(taskDef)
        for qId, quantum in enumerate(quanta):

            # node for a task
            taskNodeName = "task_{}_{}".format(taskId, qId)
            _renderTaskNode(taskNodeName, taskDef, file)

            # quantum inputs
            for dsRefs in quantum.inputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    _renderEdge(nodeName, taskNodeName, file)

            # quantum outputs
            for dsRefs in quantum.outputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    _renderEdge(taskNodeName, nodeName, file)

    print("}", file=file)
    if close:
        file.close()


def pipeline2dot(pipeline, file):
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

    def expand_dimensions(dimensions):
        """Returns expanded list of dimensions, with special skypix treatment.

        Parameters
        ----------
        dimensions : `list` [`str`]

        Returns
        -------
        dimensions : `list` [`str`]
        """
        dimensions = set(dimensions)
        skypix_dim = []
        if "skypix" in dimensions:
            dimensions.remove("skypix")
            skypix_dim = ["skypix"]
        dimensions = universe.extract(dimensions)
        return list(dimensions.names) + skypix_dim

    # open a file if needed
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    print("digraph Pipeline {", file=file)

    allDatasets = set()
    if isinstance(pipeline, Pipeline):
        pipeline = pipeline.toExpandedPipeline()
    for idx, taskDef in enumerate(pipeline):

        # node for a task
        taskNodeName = "task{}".format(idx)
        _renderTaskNode(taskNodeName, taskDef, file, idx)

        for attr in iterConnections(taskDef.connections, 'inputs'):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr.dimensions)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            _renderEdge(attr.name, taskNodeName, file)

        for attr in iterConnections(taskDef.connections, 'prerequisiteInputs'):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr.dimensions)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            # use dashed line for prerequisite edges to distinguish them
            _renderEdge(attr.name, taskNodeName, file, style="dashed")

        for attr in iterConnections(taskDef.connections, 'outputs'):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr.dimensions)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            _renderEdge(taskNodeName, attr.name, file)

    print("}", file=file)
    if close:
        file.close()
