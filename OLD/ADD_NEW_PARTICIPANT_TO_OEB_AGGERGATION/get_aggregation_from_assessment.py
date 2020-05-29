import io, json
import os

challenges_hash={}
with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/challenges_oeb.1.json", 'r') as f:
        data = json.load(f)
        for item in data:
            challenges_hash[item["_id"]] = item["orig_id"]

with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/ADD_NEW_PARTICIPANT_TO_OEB_AGGERGATION/assessment_data_Broccoli_2018.json", 'r') as f:
    oeb_assessment_data = json.load(f)
        

with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/2018_missing_participants/broccoli/Assessment_datasets_broccoli.json", 'r') as f:
    assessment_data = json.load(f)

with open("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/ADD_NEW_PARTICIPANT_TO_OEB_AGGERGATION/Dataset_aggregation_april_2020.json", 'r') as f:
    oeb_aggregation_data = json.load(f)
    for item in oeb_aggregation_data:
        
        value_x = None
        value_y = None

        challenge = challenges_hash[ item["challenge_ids"][0]]
        metric_x = item["datalink"]["inline_data"]["visualization"]["x_axis"] 
        metric_y = item["datalink"]["inline_data"]["visualization"]["y_axis"] 
        print (challenge)
        for dataset in assessment_data:
            if "QfO:2018-07-07_" + dataset["challenge_id"] in challenge:
                if dataset["metrics"]["metric_id"] in metric_x:
                    value_x = dataset["metrics"]["value"]
                    stderr_x =  dataset["metrics"]["stderr"]
                    for element in oeb_assessment_data:
                        if "QfO:2018-07-07_" + dataset["challenge_id"] in element["orig_id"] and dataset["metrics"]["metric_id"] in element["orig_id"]:
                            dataset_1 = element["_id"]
                if dataset["metrics"]["metric_id"] in metric_y:
                    value_y = dataset["metrics"]["value"]
                    stderr_y =  dataset["metrics"]["stderr"]
                    for element in oeb_assessment_data:
                        if "QfO:2018-07-07_" + dataset["challenge_id"] in element["orig_id"] and dataset["metrics"]["metric_id"] in element["orig_id"]:
                            dataset_2 = element["_id"]
            if value_x != None and value_y != None:
                new_participant_object = {
                    "metric_x": value_x,
                    "metric_y": value_y,
                    "tool_id": dataset["participant_id"],
                    "stderr_x": stderr_x,
                    "stderr_y": stderr_y
                }
                item["datalink"]["inline_data"]["challenge_participants"].append(new_participant_object)
                item["depends_on"]["rel_dataset_ids"].extend([
                    {"dataset_id": dataset_1},{"dataset_id":dataset_2}
                ])
		#print (value_x, value_y, stderr_x, stderr_y)
                value_x = None
                value_y = None
		stderr_x = None
		stderr_y = None

    with open(os.path.join("upload.json"), 'w') as f:
        json.dump(oeb_aggregation_data, f, sort_keys=True, indent=4, separators=(',', ': '))
