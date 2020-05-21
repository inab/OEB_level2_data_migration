import os
import io, json
import pandas


def read_files (in_dir, challenges_oeb, tools_oeb, metrics_oeb, out_dir):

    final_assessment =[]
    for name in os.listdir(in_dir):
        with open(os.path.join(in_dir, name), 'r') as f:
            data = json.load(f)
            # replace old_ids with oeb ids
            print (data["_id"])
            data["_id"] = "QfO:2018-07-07_" + data["_id"]
            if data["challenge_ids"][0] in challenges_hash:
                data["challenge_ids"][0] = challenges_hash[data["challenge_ids"][0]]
            if str(data["depends_on"]["metrics_id"]) in metrics_hash:
                data["depends_on"]["metrics_id"] = metrics_hash[str(data["depends_on"]["metrics_id"])]
            if str(data["depends_on"]["tool_id"]) in tools_hash:
                data["depends_on"]["tool_id"] = tools_hash[str(data["depends_on"]["tool_id"])]

            final_assessment.append(data)

    filename = "Data_model_assessments_QfO_2018.json"
    with open(os.path.join(out_dir, filename), 'w') as f:
        json.dump(final_assessment, f, sort_keys=True, indent=4, separators=(',', ': '))
if __name__ == "__main__":


   # Assuring the output directory does exist
    out_dir = "out/Broccoli_2018/final_assessment_file"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    in_dir = "/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/out/Broccoli_2018/assessment_datasets"

    
    metrics_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/metrics_oeb.json", 'r') as f:
            data = json.load(f)
            for item in data:
                metrics_hash[item["orig_id"]] = item["_id"]
    tools_hash={}

    data = pandas.read_csv("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/tools_oeb.csv", sep='\t')
    for i, tool in enumerate(data.iloc[:, 0]):
        tools_hash[data.iloc[i, 0]] = data.iloc[i, 1]
    
    challenges_hash={}
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/challenges_oeb.1.json", 'r') as f:
            data = json.load(f)
            for item in data:
                challenges_hash[item["orig_id"]] = item["_id"]
    read_files(in_dir, challenges_hash, tools_hash, metrics_hash, out_dir)
