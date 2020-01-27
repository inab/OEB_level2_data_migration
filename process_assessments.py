import os
import io, json

def build_json_schema (community_ids, challenge_name, challenge_id, participant, full_method_name, metric_name, metric_value, error_value):

    info = {

        "_id": challenge_id + "_A_" + metric_name + "_" + participant,
        "description": "Assessment dataset for applying Metric '" + metric_name + "' to " + participant + " predictions in " +
                       full_method_name,
        "dates": {
            "creation": "2018-07-07T00:00:00Z",
            "modification": "2018-07-07T14:00:00Z"
        },
        "type": "assessment",
        "visibility": "public",
        "datalink": {
            "inline_data": {"value": metric_value, "error": error_value}
        },
        "depends_on": {
            "tool_id": "QfO:" + participant,
            "metrics_id": "QfO:" + metric_name,
            "rel_dataset_ids": [
                {
                    "dataset_id": "OEBD0020000000",
                }
            ]
        },
        "_schema": "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset",
        "community_ids": [community_ids],
        "challenge_ids": [challenge_id],
        "version": "1",
        "name": "Assesment of " + metric_name + " in " + participant,
        "dataset_contact_ids": [
            "Christophe.Dessimoz",
            "Adrian.Altenhoff"
        ]
    }
    # print info
    filename = "Dataset_assessment_" + challenge_id + "_" + participant + "_" + metric_name + ".json"
    # print filename

    with open(os.path.join(out_dir, filename), 'w') as f:
        json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))

def read_workflow_assessment (in_dir, out_dir):

    for name in os.listdir(in_dir):
        with open(os.path.join(in_dir, name), 'r') as f:
            assess_array = json.load(f)
        for data in assess_array:
            # if "EC" not in data["challenge_id"]:
                if data["metrics"]["metric_id"] == "TPR":
                    if "SwissTree" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="SwissTree_TPR"
                    elif "TreeFam-A" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="TreeFam-A_TPR"
                elif data["metrics"]["metric_id"] == "PPV":
                    if "SwissTree" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="SwissTree_PPV"
                    elif "TreeFam-A" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="TreeFam-A_PPV"
                elif data["metrics"]["metric_id"] == "NR_ORTHOLOGS":
                    if "GO" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="GOTest_ortholog_relations"
                    elif "EC" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="ECTest_ortholog_relations"
                    elif "STD" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="STD_num_orthologs"
                elif data["metrics"]["metric_id"] == "avg Schlicker":
                    if "GO" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="GOTest_schlicker"
                    elif "EC" in data["challenge_id"]:
                        data["metrics"]["metric_id"]="ECTest_schlicker"

                    # get required key values
                community_ids = "OEBC002"
                challenge_name = data["challenge_id"]
                challenge_id =  data["challenge_id"]
                participant = data["participant_id"]
                full_method_name = data["challenge_id"]
                metric_name = data["metrics"]["metric_id"]
                metric_value = data["metrics"]["value"]
                error_value = data["metrics"]["stderr"]
                build_json_schema (community_ids, challenge_name, challenge_id, participant, full_method_name, metric_name, metric_value, error_value)


if __name__ == "__main__":


   # Assuring the output directory does exist
    out_dir = "out/assessment_datasets/EGGNOG"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    in_dir = "/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/all_assessments/NEW_PARTICIPANT_EGGNOG"

    read_workflow_assessment(in_dir, out_dir)