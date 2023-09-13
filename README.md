# HAPI Pipelines

[![Build Status](https://github.com/OCHA-DAP/hapi-pipelines/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hapi-pipelines/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hapi-pipelines/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hapi-pipelines?branch=main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

HAPI is a process that runs ...

For more information, please read the [documentation](https://hapi-pipelines.readthedocs.io/en/latest/).

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have
humanitarian related data, please upload your datasets to HDX.

## Running

Ensure the `hapi` library is installed using `pip install .`.

Use the
[HAPI schema repository](https://github.com/OCHA-DAP/hapi-schemas)
to create an empty SQLite HAPI database. Point to the file
`hapi-schemas/databases/hapi-test.sqlite` when executing:

```shell
python src/hapi/pipelines/app/__main__.py --db "sqlite:///path-to-hapi-test.sqlite"
```
