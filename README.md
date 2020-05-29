# OEB_workflows_data_migration (BETA version)
## Description
Application used by community managers to migrate results of a benchmarking workflow from [Virtual Research Environment](https://openebench.bsc.es/vre) to [OpenEBench](https://openebench.bsc.es) database. It takes the minimal datasets from the 'consolidated results' from the workflow, adds the rest of metadata to validate against the [Benchmarking Data Model](https://github.com/inab/benchmarking-data-model), and the required OEB keys, and pushes them to OpenEBench temporary database.

## Prerequisites for moving workflow results from VRE to OEB
In order to use the migration tool, some requirements need to be fulfilled:
* The benchmarking event and challenges that the results refer to should already be registered in OpenEBench and have official OEB identifiers.
* The tool that computed the input file' predictions should also be registered in OpenEBench.
* The 'consolidated results' file should come from a pipeline that follows the OpenEBench Benchmarking Workflows Standards.
(If any of these requirements is not satisfied, a form should be provided so that the manager or developer can 'inaugurate' the required object in OEB)

## Parameters

## Usage