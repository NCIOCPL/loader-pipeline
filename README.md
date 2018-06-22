# loader-pipeline
This is a very simple ETL library for creating NodeJS based loaders.

## Main Process



## Step Types

### Source Step
The source step's purpose is to generate a set of items to be processed, by fetching all of those items and returning a collection of raw records to be processed.
NOTES:
* The expected return from this step's getResources method is a full collection of the records to be processed.  Obviously is this memory intensive.
  * In the future this step should work as a generator allowing the main processor to grab batches of items, OR,
  * It should generate a list of items to work on and a fetcher step can be introduced.
* There can be only 1 Source Step

### Transformer Step
This step's purpose is to transform the records for loading into the loader step.
NOTES:
* There can be multiple transformer steps

### Loader Step
A loader step is the task that stores a single record to the final data store. Its main core logic is performed in the loadResource method.
NOTES:
* There can be multiple loader steps


