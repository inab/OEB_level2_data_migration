import os
import io, json
import pandas


def run(tools_oeb_hash, links_hash, out_dir):

    for tool_name, link in links_hash.items():

        info = {
            "_id": "QfO:2018-07-07_P_" + tool_name,
            "name": "Orthologs predicted by " + tool_name,
            "description": "List of orthologs pairs predicted by " + tool_name + " using the Quest for Orthologs reference proteome",
            "dates": {
                "creation": "2018-07-07T00:00:00Z",
                "modification": "2018-07-07T14:00:00Z"
            },
            "datalink": {
                "uri": link,
                "attrs": ["archive"],
                "validation_date": "2018-07-07T00:00:00Z",
                "status": "ok"
            },
            "type": "participant",
            "visibility": "public",
            "_schema": "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset",
            "community_ids": ["OEBC002"],
            "challenge_ids": [
                                "OEBX002000000A",
                                "OEBX002000000B",
                                "OEBX002000000C",
                                "OEBX002000000D",
                                "OEBX002000000E",
                                "OEBX002000000F",
                                "OEBX002000000G",
                                "OEBX002000000H",
                                "OEBX002000000I",
                                "OEBX002000000J",
                                "OEBX002000000O",
                                "OEBX002000000K",
                                "OEBX002000000L",
                                "OEBX002000000M",
                                "OEBX002000000N"
                            ],
            "depends_on": {
                "tool_id": tools_oeb_hash[tool_name],
                "rel_dataset_ids": [
                    {
                        "dataset_id": "OEBD00200001FC",
                    }
                ]
            },
            "version": "unknown",
            "dataset_contact_ids": [
                "Adrian.Altenhoff",
                "Christophe.Dessimoz"
            ]
        }

        # print info
        filename = "Dataset_participant_" + tool_name + "_2018.json"
        # print filename

        with open(os.path.join(out_dir, filename), 'w') as f:
            json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":


   # Assuring the output directory does exist
    out_dir = "out/participant_datasets/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    tools_oeb_hash={}
    data = pandas.read_csv("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/tools_oeb_no_prefix.csv", sep='\t')
    for i, tool in enumerate(data.iloc[:, 0]):
        tools_oeb_hash[data.iloc[i, 0]] = data.iloc[i, 1]

    links_hash={}
    data = pandas.read_csv("/home/jgarrayo/benchmark_repositories/QFO_data_model_2018/participant_links.tsv", sep='\t')
    for i, tool in enumerate(data.iloc[:, 0]):
        links_hash[data.iloc[i, 0]] = data.iloc[i, 1]

    run( tools_oeb_hash, links_hash, out_dir)