"""CLI script to disambiguate entities with Aida-light.

Usage:
    link-entities.py --input-dir=<inp> --output-dir=<out> --files-range=<r> [--gateway-port=<p> --scheduler=<ds>]

Options:
--input-dir=<inp>
--output-dir=<out>
--files-range=<r>
--gateway-port=<p>
"""  # noqa: E501

import json
import os
from time import strftime
from docopt import docopt
from dask.distributed import Client
from dask import bag as db
from py4j.java_gateway import JavaGateway, GatewayParameters
from dask.distributed import Client

# from impresso_images.data_utils import fixed_s3fs_glob


def aida_disambiguate_documents(documents, gateway_port):

    # do py4j business
    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=gateway_port))
    aida_server = gateway.entry_point.getAida()

    output = []
    print(f"Linking partition with {len(documents)} documents [{', '.join([doc['id'] for doc in documents])}]")

    for document in documents:

        prepared_input = document['input']
        mentions = gateway.entry_point.findExtractedMentions(prepared_input)
        annotations = aida_server.disambiguate(prepared_input, mentions, "fullSettings")

        output_doc = {"id": document['id'], "sys_id": "aidalight-fullSettings", "cdt": strftime("%Y-%m-%d %H:%M:%S")}
        linked_entities = []

        annotations = [
            {
                "surface": ann.getMention(),
                "entity_id": annotations.get(ann).getName(),
                "normalized_name": annotations.get(ann).getNMEnormalizedName(),
                "char_offset": ann.getCharOffset(),
                "mention_entity_similarity": annotations.get(ann).getMentionEntitySimilarity(),
            }
            for ann in annotations.keySet()
        ]

        for mention in document['nes']:

            # since some character offsets are broken we rely
            # on the surface to realign mention and disambiguations
            matching_entity = [annotation for annotation in annotations if annotation['surface'] == mention['surface']]

            if len(matching_entity) == 0:
                continue
            else:
                matching_entity = matching_entity[0]

            # import ipdb; ipdb.set_trace()

            entity_id = matching_entity['entity_id']
            linked_entities.append(
                {
                    "mention_id": mention['id'],
                    "surface": mention['surface'],
                    "entity_id": entity_id,
                    "entity_link": f"http://en.wikipedia.org/wiki/{entity_id}",
                    "normalized_name": matching_entity['normalized_name'],
                    "mention_entity_similarity": matching_entity["mention_entity_similarity"],
                }
            )
        output_doc['ne_links'] = linked_entities
        output.append(output_doc)

    return output


def main():
    arguments = docopt(__doc__)
    local_input_dir = arguments["--input-dir"]
    local_output_dir = arguments["--output-dir"]
    dask_scheduler_port = arguments["--scheduler"]

    port = arguments["--gateway-port"]
    gateway_port = 25333 if not port else int(port)
    print(f"Java Gateway will be using port {gateway_port}")

    if scheduler:
        dask_client = Client(scheduler)
    else:
        dask_client = Client()
    print(f"{client}")

    range_start, range_end = arguments['--files-range'].split('-')
    range_start = int(range_start)
    range_end = int(range_end)

    aida_input_files = [
        os.path.join(local_input_dir, file)
        for file in os.listdir(local_input_dir)
        if int(file.replace(".bz2", "")) >= range_start and int(file.replace(".bz2", "")) < range_end
    ]

    aida_output_files = [os.path.join(local_output_dir, os.path.basename(file)) for file in aida_input_files]

    print(f"Number of input files: {len(aida_input_files)}")
    print(f"Number of input files: {len(aida_output_files)}")

    print(f"First 10 input files: {aida_input_files[:10]}")
    print(f"First 10 output files: {aida_output_files[:10]}")

    # set to a certain number of workers
    dask_client = Client(processes=False, n_workers=8)
    print(f"{dask_client}")

    aida_input_bag = (
        db.read_text(
            aida_input_files,
            # storage_options=IMPRESSO_STORAGEOPT
        )
        .map(json.loads)
        .map_partitions(aida_disambiguate_documents, gateway_port)
        .map(json.dumps)
        .to_textfiles(aida_output_files)
    )

    print(aida_input_bag)


if __name__ == '__main__':
    main()
