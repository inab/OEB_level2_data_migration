"""
#########################################################
	    VRE Level 2 to OpenEBench migration tool 
		Author: Javier Garrayo Ventas
		Barcelona Supercomputing Center. Spain. 2020
#########################################################
"""
from process.participant import participant
import json
from argparse import ArgumentParser

def main(input_file, data_visibility, bench_event_id, file_location, community_id, tool_id):

    with open(input_file, 'r') as f:
        data = json.load(f)

    for dataset in data:

        if "type" in dataset and dataset["type"] == "participant":
            participant_data = dataset

    process_participant = participant()
    process_participant.build_participant_dataset(participant_data, data_visibility, bench_event_id, file_location, community_id, tool_id)

    #build_test_events()

    ##VALIDATE!! JM validator


if __name__ == '__main__':
    
    parser = ArgumentParser()
    parser.add_argument("-i", "--consolidated_oeb_data", help="json file with the aggregation of datasets coming from a OEB VRE workflow \
                                                                    (data_type:consolidated_benchmark_dataset)", required=True)
    parser.add_argument("-v", "--data_visibility", help="visibility of the datasets associated to the participant's run, according \
                                                                    to the benchmarking data model - 'enum': [ 'public', 'community', 'challenge', 'participant' ]", required=True)
    parser.add_argument("-be", "--benchmarking_event_id", help="benchmarking event id that corresponds to the executed workflow \
                                                                    - should be an official OEB id stored in the DB", required=True)
    parser.add_argument("-f", "--participant_file", help="location of the file that was uploaded by the participant \
                                                                    - should be a FS path or a remote DOI", required=True)
    parser.add_argument("-com", "--community_id", help=" official id of the community that corresponds to the execution \
                                                                    - should already be registered in OEB", required=True)
    parser.add_argument("-t", "--tool_id", help=" official id of the tool that made the predictions which were used as input \
                                                    , if tool is not registered in OEB, should provide the access to a form to register it", required=True)                                                                   

                                                                
    args = parser.parse_args()

    input_file = args.consolidated_oeb_data
    data_visibility = args.data_visibility
    bench_event_id = args.benchmarking_event_id
    file_location = args.participant_file
    community_id = args.community_id
    tool_id = args.tool_id

    
    main(input_file, data_visibility, bench_event_id, file_location, community_id, tool_id)
