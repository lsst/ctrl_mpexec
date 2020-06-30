.. py:currentmodule:: lsst.ctrl.mpexec

.. _lsst.ctrl.mpexec:

################
lsst.ctrl.mpexec
################

.. Paragraph that describes what this Python module does and links to related modules and frameworks.

.. .. _lsst.ctrl.mpexec-using:

.. Using lsst.ctrl.mpexec
.. ======================

.. toctree linking to topics related to using the module's APIs.

.. .. toctree::
..    :maxdepth: 1

.. _lsst.ctrl.pipetask-script:

Command Line Scripts
====================

The `pipetask` command is being ported from an argparse framework to a Click
framework. During development the command implemented using Click is called
`pipetask2`. At some point the current `pipetask` command will be removed and
`pipetask2` will be renamed to `pipetask`.

.. click:: lsst.ctrl.mpexec.cli.pipetask:cli
   :prog: pipetask2
   :show-nested:

.. _lsst.ctrl.mpexec-contributing:

Contributing
============

``lsst.ctrl.mpexec`` is developed at https://github.com/lsst/ctrl_mpexec.
You can find Jira issues for this module under the `ctrl_mpexec <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20ctrl_mpexec>`_ component.

.. If there are topics related to developing this module (rather than using it), link to this from a toctree placed here.

.. .. toctree::
..    :maxdepth: 1

.. _lsst.ctrl.mpexec-pyapi:

Python API reference
====================

.. automodapi:: lsst.ctrl.mpexec
   :no-main-docstr:
   :no-inheritance-diagram:
