import os
import io, json
import pandas


def run(dataset, out_dir, challenges_hash, tools_oeb_hash, assessments_oeb_hash):

    challenge_name = dataset["challenge_ids"][0]
    tool_name = dataset["depends_on"]["tool_id"]
    metric_name = dataset["depends_on"]["metrics_id"]
    assessment_oeb_id = assessments_oeb_hash[dataset["_id"]]

    info = {

        "_id": challenge_name + "_metricsEvent_" + tool_name.split(":")[1] + "_" + metric_name.split(":")[1],
        "_schema": "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/TestAction",
        "action_type": "MetricsEvent",
        "tool_id": tools_oeb_hash[tool_name],
        "involved_datasets": [
            {
                "dataset_id": "QfO:2018-07-07_P_" + tool_name.split(":")[1],
                "role": "incoming"
            },
            {
                "dataset_id": assessment_oeb_id,
                "role": "outgoing"
            }
        ],
        "challenge_id": challenges_hash[challenge_name],
        "dates": {
            "creation": "2018-07-07T00:00:00Z",
                        "reception": "2018-07-07T00:00:00Z",
        },
        "test_contact_ids": [
            "Adrian.Altenhoff",
            "Christophe.Dessimoz"
        ]
    }   
    # print info
    filename = "MetricsEvent_" + challenge_name + "_" + tool_name + "_" + metric_name + ".json"
    # print filename

    with open(os.path.join(out_dir, filename), 'w') as f:
        json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":


   # Assuring the output directory does exist
    out_dir = "out/metrics_events/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    tools_oeb_hash={}
    data = pandas.read_csv("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/tools_oeb.csv", sep='\t')
    for i, tool in enumerate(data.iloc[:, 0]):
        tools_oeb_hash[data.iloc[i, 0]] = data.iloc[i, 1]

    challenges_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/challenges_oeb.json", 'r') as f:
            data = json.load(f)
            for item in data:
                challenges_hash[item["orig_id"]] = item["_id"]
    assessments_oeb_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/assessments_oeb/dev-oeb-scientific.Dataset.json", 'r') as f:
            data = json.load(f)
            for item in data:
                assessments_oeb_hash[item["orig_id"]] = item["_id"]
    
    in_dir = "/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/out/assessment_datasets/OLD+EGGNOG"
    for name in os.listdir(in_dir):
        with open(os.path.join(in_dir, name), 'r') as f:
            dataset = json.load(f)
            
            run(dataset, out_dir, challenges_hash, tools_oeb_hash, assessments_oeb_hash)
