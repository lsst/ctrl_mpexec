#!/bin/env python

"""Scripts that creates QuantumGraph "manually".
"""

from __future__ import absolute_import, division, print_function

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from argparse import ArgumentParser, FileType
import logging
import pickle
import sys

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler.core.datasets import DatasetRef
from lsst.daf.butler.core.quantum import Quantum
from lsst.daf.butler.core.run import Run
from lsst.pipe.supertask import (Pipeline, QuantumGraph, QuantumGraphNodes,
                                 TaskDef, SuperTask)
from lsst.pipe.supertask.pipeTools import orderPipeline
from lsst.pipe.supertask.examples import test1task, test2task
# this is not used but need to be imported to register a storage class
from lsst.pipe.supertask.examples.exampleStorageClass import ExampleStorageClass  # noqa: F401

# ---------------------
#  Local definitions --
# ---------------------


def _configLogger(verbosity):
    """ configure logging based on verbosity level """

    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logfmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    logging.basicConfig(level=levels.get(verbosity, logging.DEBUG), format=logfmt)

# ---------------------------
#  Main application method --
# ---------------------------


def main():

    descr = 'One-line application description.'
    parser = ArgumentParser(description=descr)
    parser.add_argument('-v', '--verbose', dest='verbose',
                        action='count', default=0,
                        help='More verbose output, can use several times.')
    parser.add_argument('-p', '--pipeline', dest='pipeline', default=None,
                        type=FileType("wb"),
                        help='Name of the output file for Pipeline pickle dump.')
    parser.add_argument('-g', '--qgraph', dest='qgraph', default=None,
                        type=FileType("wb"),
                        help='Name of the output file for QunatumGraph pickle dump.')
    args = parser.parse_args()

    # configure logging
    _configLogger(args.verbose)

    #
    #  Application logic goes here
    #
    if args.pipeline is None and args.qgraph is None:
        parser.error("Need one of -p or -g options")

    if args.pipeline:

        pipeline = Pipeline([_makeStep1TaskDef(), _makeStep2TaskDef(), _makeStep3TaskDef()])

        # make sure it is ordered, this is joust to test some methods in supertasks
        pipeline = orderPipeline(pipeline)

        # save to file
        pickle.dump(pipeline, args.pipeline)

    if args.qgraph:

        run = Run(collection=1, environment=None, pipeline=None)

        step1 = _makeStep1TaskDef()
        step2 = _makeStep2TaskDef()
        step3 = _makeStep3TaskDef()

        dstype0 = SuperTask.makeDatasetType(step1.config.input)
        dstype1 = SuperTask.makeDatasetType(step1.config.output)
        dstype2 = SuperTask.makeDatasetType(step2.config.output)
        dstype3 = SuperTask.makeDatasetType(step3.config.output)

        # quanta for first step which is 1-to-1 tasks
        quanta = []
        for visit in range(10):
            quantum = Quantum(run=run, task=None)
            quantum.addPredictedInput(_makeDSRefVisit(dstype0, visit))
            quantum.addOutput(_makeDSRefVisit(dstype1, visit))
            quanta.append(quantum)
        step1nodes = QuantumGraphNodes(step1, quanta)

        # quanta for second step which is 1-to-1 tasks
        quanta = []
        for visit in range(10):
            quantum = Quantum(run=run, task=None)
            quantum.addPredictedInput(_makeDSRefVisit(dstype1, visit))
            quantum.addOutput(_makeDSRefVisit(dstype2, visit))
            quanta.append(quantum)
        step2nodes = QuantumGraphNodes(step2, quanta)

        # quanta for third step which is M-to-N
        patch2visits = (
            (1, 1, (0, 1, 2, 3)),
            (1, 2, (2, 3, 4, 5)),
            (1, 3, (4, 5, 6, 7)),
            (2, 0, (6, 7, 8, 9)),
        )
        quanta = []
        for tract, patch, visits in patch2visits:
            quantum = Quantum(run=run, task=None)
            for visit in visits:
                quantum.addPredictedInput(_makeDSRefVisit(dstype2, visit))
            quantum.addOutput(_makeDSRefPatch(dstype3, tract, patch))
            quanta.append(quantum)
        step3nodes = QuantumGraphNodes(step2, quanta)

        qgraph = QuantumGraph([step1nodes, step2nodes, step3nodes])
        pickle.dump(qgraph, args.qgraph)


def _makeDSRefVisit(dstype, visitId):
        return DatasetRef(datasetType=dstype,
                          dataId=dict(Camera="X",
                                      Visit=visitId,
                                      physical_filter='f',
                                      abstract_filter='f'))


def _makeDSRefPatch(dstype, tractId, patchId):
        return DatasetRef(datasetType=dstype,
                          dataId=dict(skymap=1,
                                      tract=tractId,
                                      patch=patchId))


def _makeStep1TaskDef():
    """Make TaskDef for first step of a pipeline"""
    config = test1task.Test1Config()
    config.input.name = "input"
    config.output.name = "step1output"

    taskDef = TaskDef(taskName="lsst.pipe.supertask.examples.test1task.Test1Task",
                      config=config,
                      taskClass=test1task.Test1Task,
                      label="step1")
    return taskDef


def _makeStep2TaskDef():
    """Make TaskDef for second step of a pipeline"""
    config = test1task.Test1Config()
    config.input.name = "step1output"
    config.output.name = "step2output"

    taskDef = TaskDef(taskName="lsst.pipe.supertask.examples.test1task.Test1Task",
                      config=config,
                      taskClass=test1task.Test1Task,
                      label="step2")
    return taskDef


def _makeStep3TaskDef():
    """Make TaskDef for third step of a pipeline"""
    config = test2task.Test2Config()
    config.quantum.sql = None
    config.input.name = "step2output"
    config.output.name = "output"

    taskDef = TaskDef(taskName="lsst.pipe.supertask.examples.test2task.Test2Task",
                      config=config,
                      taskClass=test2task.Test2Task,
                      label="step3")
    return taskDef


#
#  run application when imported as a main module
#
if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
