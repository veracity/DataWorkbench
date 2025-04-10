<picture align="center">
  <source media="(prefers-color-scheme: dark)" srcset="https://cdn.veracity.com/common/icons/logos/veracity-logo.svg">
  ![Veracity DataWorkbench Logo](https://cdn.veracity.com/common/icons/logos/veracity-logo.svg)
</picture>

-----------------

# Veracity DataWorkbench Python

| | |
| --- | --- |
| Testing | [![CI](https://github.com/veracity/DataWorkbench/actions/workflows/ci.yml/badge.svg)](https://github.com/veracity/DataWorkbench/actions/workflows/ci.yml) |


# DataWorkbench

## What is it?
Veracity DataWorkbench is a Python SDK designed to bridge your Databricks environment with Veracity Data Workbench. It simplifies access to data cataloging, lineage tracking, and APIs.


## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [How to use it](#how-to-use-it)
- [Configuration](#configuration)
- [Examples](#examples)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

## Features
- **DataCatalogue**: Register and manage datasets in the Veracity Data Workbench Data Catalogue.

## Installation
This package is pre-installed in Veracity-hosted Databricks environments (if analytics features are enabled).

To install the latest version locally:

```sh
pip install https://github.com/veracity/DataWorkbench/releases/latest/download/dataworkbench-1.0-py3-none-any.whl
```
Make sure you have the required credentials and environment variables set when running outside Databricks.


## How to use it
In Veracity-hosted Databricks, the SDK is ready to use:

```python
import dataworkbench
```

To use it on your local machine, it requires you to set a set of variables to connect to the Veracity Dataworkbench API.

### Basic Example

```python
from dataworkbench import DataCatalogue

df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["letter", "number"])

datacatalogue = DataCatalogue()  # Naming subject to change
datacatalogue.save(df, "Dataset Name", "Description", tags={"environment": ["test"]})
```

## Examples

### Saving a Spark DataFrame to the Data Catalogue

```python
from dataworkbench import DataCatalogue

df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["letter", "number"])

datacatalogue = DataCatalogue()  # Naming subject to change
datacatalogue.save(df, "Dataset Name", "Description", tags={"environment": ["test"]})
```

## API Reference

### DataCatalogue

- `save(df, name, description=None, tags=None)`: Save a Spark DataFrame to the Data Workbench Data Catalogue


## License

Dataworkbench is licensed under [WHICH LICENSE](LICENSE).
