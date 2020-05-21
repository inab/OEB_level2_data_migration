# OEB_level2_data_migtration
Example data and code used to migrate the outputs of QfO 2018 workflow to the OEB benchmarking data model.
The scripts found in this repository are just in BETA state, but might be used in the future in the migration tool from VRE to OpenEBench.

1. process_assessments.py reads the assessent data coming from the workflow and adds the rests of metadata, for compatibility  with the benchmarking data model
2. rename_oeb_ids.py takes the output from the previous step and substitutes the orig_ids with the official OEB, if it finds some that are already registered in the database.
3. process_aggregations.py does the same with the aggregation datasets coming from the workflow.
4. the get_***_schemas.py scripts are used to generate the required TestActions that connect the datasets generated in the previous scripts.
5. In case, an aggregation dataset that is already uploaded to OEB needs to be updated, use the example in ADD_NEW_PARTICIPANT_TO_OEB_AGGREGATION
