import os
import io, json
import pandas


def run (in_dir, out_dir):

    for name in os.listdir(in_dir):

        #generate array with all incoming assessment datasets aun aoutgoing aggregation dataset
        with open(os.path.join(in_dir, name), 'r') as f:
            aggregation_data = json.load(f)

        involved_datasets = []
        iter_datasets = iter(aggregation_data["depends_on"]["rel_dataset_ids"])
        next(iter_datasets)
        for dataset in iter_datasets:
            dataset["role"] = "incoming"
            involved_datasets.append(dataset)
        
        involved_datasets.append({ 
            "dataset_id": aggregation_data["_id"],
            "role": "outgoing"
        })



        info = {

            "_id": aggregation_data["_id"] + "Event",
            "_schema": "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/TestAction",
            "tool_id": "OEBT0020000002",
            "action_type": "AggregationEvent",
            "involved_datasets": involved_datasets,
            "challenge_id": aggregation_data["challenge_ids"][0],
            "dates":{
                "creation": "2018-07-07T00:00:00Z",
                "reception": "2018-07-07T00:00:00Z"
            },
            "test_contact_ids": [
                "Adrian.Altenhoff",
                "Christophe.Dessimoz"
            ]
        }

        # print info
        filename = "AggregationEvent_" + aggregation_data["_id"] + ".json"
        # print filename

        with open(os.path.join(out_dir, filename), 'w') as f:
            json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":


    # Assuring the output directory does exist
    out_dir = "out/aggregation_events/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    in_dir = "/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/out/aggregation_datasets"


    run(in_dir, out_dir)