import os
import io, json
import pandas


def run(tools_oeb_hash, challenges_hash, out_dir):
    
    for tool_name, oeb_id in tools_oeb_hash.items():
        for challenge_id, challenge_name in challenges_hash.items():

            info = {

                    "_id": challenge_name + "_testEvent_" + tool_name,
                    "_schema":"https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/TestAction",
                    "action_type":"TestEvent",
                    "tool_id": oeb_id,
                    "involved_datasets":[
                        {
                            "dataset_id": "OEBD00200001FC",
                            "role": "incoming"
                        },
                        {
                            "dataset_id": "QfO:2018-07-07_P_" + tool_name,
                            "role": "outgoing"
                        }

                    ],
                    "challenge_id": challenge_id,
                    "dates":{
                        "creation": "2020-01-30T00:00:00Z",
                        "reception": "2020-01-30T00:00:00Z",
                    },
                    "test_contact_ids": [
                        "Adrian.Altenhoff",
                        "Christophe.Dessimoz"
                    ]
            }

            # print info
            filename = "TestEvent_" + challenge_name + "_" + tool_name + ".json"
            # print filename

            with open(os.path.join( out_dir, filename), 'w') as f:
                json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":


   # Assuring the output directory does exist
    out_dir = "out/Broccoli_2018/test_events/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    tools_oeb_hash={}
    data = pandas.read_csv("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/new_tools_2018_oeb.csv", sep='\t')
    for i, tool in enumerate(data.iloc[:, 0]):
        tools_oeb_hash[data.iloc[i, 0]] = data.iloc[i, 1]

    challenges_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/challenges_oeb.json", 'r') as f:
            data = json.load(f)
            for item in data:
                challenges_hash[item["_id"]] = item["orig_id"]

    run( tools_oeb_hash, challenges_hash, out_dir)
