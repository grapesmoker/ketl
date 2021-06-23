====
kETL
====


.. image:: https://img.shields.io/pypi/v/ketl.svg
        :target: https://pypi.python.org/pypi/ketl

.. image:: https://img.shields.io/travis/grapesmoker/ketl.svg
        :target: https://travis-ci.com/grapesmoker/ketl

.. image:: https://readthedocs.org/projects/ketl/badge/?version=latest
        :target: https://ketl.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: images/kettle.jpg
        :target: https://www.clevelandart.org/art/1976.23
        :alt: A picture of a kettle.


konfigurable ETL


* Free software: MIT license
* Documentation: https://ketl.readthedocs.io.


Introduction
------------

kETL is a konfigurable ETL library. Its job is to simplify the process of extracting, transforming,
and loading files from the internet. The goal of kETL is not to be all things to all use cases,
but rather to maintain a simple but useful set of features intended to get your data from wherever
it lives to some place where you can do stuff with it.

Features
--------

* Strict separation of extraction, transformation, and loading concerns
* Easily configurable for most use cases; derive subclasses to define your own behaviors
* Simple API to use with or without pipelining logic

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
