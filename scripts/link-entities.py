"""CLI script to disambiguate entities with Aida-light.

Usage:
    link-entities.py --input-dir=<inp> --output-dir=<out> --files-range=<r> [--gateway-port=<p>]

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

# from impresso_images.data_utils import fixed_s3fs_glob


def aida_disambiguate_documents(documents, gateway_port):

    # do py4j business
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port)
    )
    aida_server = gateway.entry_point.getAida()

    output = []

    for document in documents:

        prepared_input = document['input']
        mentions = gateway.entry_point.findExtractedMentions(prepared_input)
        annotations = aida_server.disambiguate(
            prepared_input,
            mentions,
            "fullSettings"
        )

        output_doc = {
            "id": document['id'],
            "sys_id": "aidalight-fullSettings",
            "cdt": strftime("%Y-%m-%d %H:%M:%S")
        }
        linked_entities = []

        for mention in annotations.keySet():

            annotation = annotations.get(mention)
            entity_id = annotation.getName()
            wikipedia_entity_id = f"http://en.wikipedia.org/wiki/{entity_id}"
            char_offset = mention.getCharOffset()
            char_length = mention.getCharLength()

            # uncomment for debug
            # print(document['ft'][char_offset:char_offset + char_length])
            # print(mention.getMention(), wikipedia_entity_id)

            try:
                matching_mention = [
                    ne
                    for ne in document['nes']
                    if ne['lOffset'] == char_offset and
                    ne['rOffset'] == char_offset + char_length
                ][0]
            except Exception:
                print(f"error in {document['id']}")
                continue

            mention_id = matching_mention['id']
            similarity = annotation.getMentionEntitySimilarity()

            linked_entities.append(
                {
                    "mention_id": mention_id,
                    "surface": matching_mention['surface'],
                    "entity_id": entity_id,
                    "entity_link": wikipedia_entity_id,
                    "normalized_name": annotation.getNMEnormalizedName(),
                    "mention_entity_similarity": similarity
                }
            )
        output_doc['ne_links'] = linked_entities
        output.append(output_doc)

    return output


def main():
    arguments = docopt(__doc__)
    local_input_dir = arguments["--input-dir"]
    local_output_dir = arguments["--output-dir"]

    port = arguments["--gateway-port"]
    gateway_port = 25333 if not port else port
    print(f"Java Gateway will be using port {gateway_port}")

    range_start, range_end = arguments['--files-range'].split('-')
    range_start = int(range_start)
    range_end = int(range_end)

    aida_input_files = [
        os.path.join(local_input_dir, file)
        for file in os.listdir(local_input_dir)
        if int(file.replace(".bz2", "")) >= range_start and
        int(file.replace(".bz2", "")) < range_end
    ]

    aida_output_files = [
        os.path.join(local_output_dir, os.path.basename(file))
        for file in aida_input_files
    ]

    print(f"Number of input files: {len(aida_input_files)}")
    print(f"Number of input files: {len(aida_output_files)}")

    print(f"First 10 input files: {aida_input_files[:10]}")
    print(f"First 10 output files: {aida_output_files[:10]}")

    # set to a certain number of workers
    dask_client = Client()
    print(f"{dask_client}")

    aida_input_bag = db.read_text(
        aida_input_files,
        # storage_options=IMPRESSO_STORAGEOPT
    ).map(json.loads).map_partitions(
        aida_disambiguate_documents,
        gateway_port
    ).map(json.dumps).to_textfiles(
        aida_output_files
    )

    print(aida_input_bag)


if __name__ == '__main__':
    main()
