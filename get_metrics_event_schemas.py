import os
import io, json
import pandas


def run(dataset, out_dir, challenges_hash, tools_oeb_hash, assessments_oeb_hash):

    challenge_name = dataset["challenge_ids"][0]
    tool_name = dataset["depends_on"]["tool_id"]
    metric_name = dataset["depends_on"]["metrics_id"]
    # assessment_oeb_id = assessments_oeb_hash[dataset["_id"]]

    dates = {
        "Proteinortho_6.0.13":"2020-01-30T00:00:00Z",
        "QfO:Proteinortho_6.0.13_with-isoform":"2020-01-30T00:00:00Z",
        "Broccoli_1.0": "2019-11-26T00:00:00Z",
	"QfO:Broccoli_1.1": "2020-04-27T00:00:00Z"
    }

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
                "dataset_id": challenge_name + "_A_"+ metric_name.split(":")[1] + "_" + tool_name.split(":")[1],
                "role": "outgoing"
            }
        ],
        "challenge_id": challenges_hash[challenge_name],
        "dates": {
            "creation": dates[tool_name],
                        "reception": dates[tool_name],
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
    out_dir = "out/Broccoli_2018/metrics_events_2018/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    tools_oeb_hash={}
    data = pandas.read_csv("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/tools_oeb.csv", sep='\t')
    for i, tool in enumerate(data.iloc[:, 0]):
        tools_oeb_hash[data.iloc[i, 0]] = data.iloc[i, 1]

    challenges_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/challenges_oeb.1.json", 'r') as f:
            data = json.load(f)
            for item in data:
                challenges_hash[item["orig_id"]] = item["_id"]
    assessments_oeb_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/assessments_oeb/dev-oeb-scientific.Dataset.json", 'r') as f:
            data = json.load(f)
            for item in data:
                assessments_oeb_hash[item["orig_id"]] = item["_id"]
    
    in_dir = "/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/out/Broccoli_2018/assessment_datasets"
    for name in os.listdir(in_dir):
        with open(os.path.join(in_dir, name), 'r') as f:
            dataset = json.load(f)
            
            run(dataset, out_dir, challenges_hash, tools_oeb_hash, assessments_oeb_hash)
