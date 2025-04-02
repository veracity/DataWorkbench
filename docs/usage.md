# Data Workbench Library Documentation

  

## Overview
The Data Workbench library provides a robust solution for storing and registering datasets in the dataworkbench ecosystem. With this library, you can easily write datasets back to dataworkbench.
  

## Installation
TODO

## Quick Start



```python

from dataworkbench.publisher import DataPublisher
  

# Create a sample DataFrame

df = spark.createDataFrame([("Alice", 30), ("Bob", 40)], ["name", "age"])

  

# Initialize the DataPublisher

publisher = DataPublisher()

  

# Save dataset

result = publisher.save_dataset(

    df=df,

    dataset_name="Customer Data",

    dataset_description="Monthly customer demographic data"

)

  

print(f"Dataset registered with ID: {result['dataset_id']}")

```

  

## Core Components

  

The library consists of the main components:

  

1. **DataPublisher**: Handles dataset storage and registration

  

## DataPublisher

  

The `DataPublisher` class is the main entry point for interacting with the library.

  

### Initialization

  

```python

from dataworkbench.publisher import DataPublisher

  

publisher = DataPublisher()

```

  

### Save a Dataset

  

```python

result = publisher.save_dataset(

    df=df,                                        # DataFrame to save

    dataset_name="Customer Data",                 # Name for the catalog

    dataset_description="Customer demographics",  # Description

    schema_id=uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),  # Optional schema ID

    tags={"environment": ["test"]},                # Optional metadata tags

    write_mode="overwrite",                       # Optional write mode (overwrite/append), default overwrite

    dataset_id=None                                # Optional dataset ID

)

```

  

#### Parameters:

- **df**: Spark DataFrame containing the dataset to be saved

- **dataset_name**: Name of the dataset for the catalog

- **dataset_description**: Description of the dataset's purpose and contents

- **schema_id** (optional): UUID for the dataset's schema

- **tags** (optional): Metadata tags as key-value pairs

- **write_mode** (optional): Storage write mode - either "overwrite" (default) or "append"

- **dataset_id** (optional): Dataset identifier, must be provided if mode is append.

  

#### Return Value:

Dictionary containing the registration details or error information.


## Best Practices  

1. **Use Descriptive Dataset Names and Descriptions**: - Makes it easier to find and understand datasets in the catalog

2. **Add Metadata Tags**: - Use tags to categorize datasets and improve searchability

3. **Use Append Mode Judiciously**: - Append mode is efficient for incremental data loads
  