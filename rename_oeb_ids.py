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
            data["challenge_ids"][0] = challenges_hash[data["challenge_ids"][0]]
            data["depends_on"]["metrics_id"] = metrics_hash[str(data["depends_on"]["metrics_id"])]
            data["depends_on"]["tool_id"] = tools_hash[str(data["depends_on"]["tool_id"])]
            final_assessment.append(data)

    filename = "EGGNOG_Data_model_assessments_QfO_2018.json"
    with open(os.path.join(out_dir, filename), 'w') as f:
        json.dump(final_assessment, f, sort_keys=True, indent=4, separators=(',', ': '))
if __name__ == "__main__":


   # Assuring the output directory does exist
    out_dir = "out/final/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    in_dir = "/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/out/assessment_datasets/EGGNOG"

    
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
    with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/challenges_oeb.json", 'r') as f:
            data = json.load(f)
            for item in data:
                challenges_hash[item["orig_id"]] = item["_id"]
    read_files(in_dir, challenges_hash, tools_hash, metrics_hash, out_dir)