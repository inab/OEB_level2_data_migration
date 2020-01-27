import os
import io, json

def build_json_schema(challenge_name, inline_data, involved_datasets, challenges_hash):

    info = {

            "_id": "QfO:2018-07-07_" + challenge_name + "_Aggregation",
            "datalink": {
                "inline_data": inline_data,
                "schema_url": "https://raw.githubusercontent.com/inab/OpenEBench_scientific_visualizer/js/benchmarking_data_model/inline_data_visualizer.json"
            },
            "type": "aggregation",
            "visibility": "public",
            "version": "unknown",
            "name": "Summary dataset for challenge: " + challenge_name,
            "description": "Summary dataset with information about challenge " + challenge_name + " (e.g. input/output datasets, metrics...)",
            "dates": {
                "creation": "2018-07-07T00:00:00Z",
                "modification": "2018-07-07T00:00:00Z"
            },
            "depends_on": {
                "tool_id": "OEBT0020000002",
                "rel_dataset_ids": involved_datasets,
            },
            "_schema": "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset",
            "community_ids": ["OEBC002"],
            "challenge_ids": [challenges_hash["QfO:2018-07-07_" + challenge_name]],
            "dataset_contact_ids": [
                "Adrian.Altenhoff",
                "Christophe.Dessimoz"
            ]
    }

    # print info
    filename = "Dataset_Aggregation_" + challenge_name + ".json"
    # print filename

    with open(os.path.join( out_dir, filename), 'w') as f:
        json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))

def run(in_dir, challenges_hash, assessments_oeb_hash, out_dir):

    for name in os.listdir(in_dir):
        involved_datasets = [{"dataset_id": "OEBD00200001FC"}]
        with open(os.path.join(in_dir, name, name + ".json"), 'r') as f:
            aggregation_data = json.load(f)
            inline_data = aggregation_data["datalink"]["inline_data"]
            challenge_name = aggregation_data["challenge_ids"][0]
            metric_x = inline_data["visualization"]["x_axis"]
            metric_y = inline_data["visualization"]["y_axis"]

            with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/all_assessments_BK/join_all_methods.json", 'r') as f:
                assessments_array = json.load(f)
                for element in assessments_array:
                    if challenge_name == element["challenge_id"]:
                        if metric_x == element["metrics"]["metric_id"] or metric_y == element["metrics"]["metric_id"]:
                            # get the OEB id for the associated assessment dataset
                            assessment_id = "QfO:2018-07-07_" + challenge_name + "_A_" + element["metrics"]["metric_id"] + "_" + element["participant_id"]
                            involved_datasets.append({ "dataset_id": assessments_oeb_hash[assessment_id]})
            build_json_schema(challenge_name, inline_data, involved_datasets, challenges_hash)
    
        

if __name__ == "__main__":


   # Assuring the output directory does exist
    out_dir = "out/aggregation_datasets/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    in_dir = "/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/final_aggregation+eggNOG"

       
    challenges_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/challenges_oeb.1.json", 'r') as f:
            data = json.load(f)
            for item in data:
                challenges_hash[item["orig_id"]] = item["_id"]

    assessments_oeb_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/assessments_oeb/dev-oeb-scientific.Dataset.1.json", 'r') as f:
            data = json.load(f)
            for item in data:
                assessments_oeb_hash[item["orig_id"]] = item["_id"]

    run(in_dir, challenges_hash, assessments_oeb_hash, out_dir)