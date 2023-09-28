import argparse
import logging
import json


import requests

PORTAL_URI = 'https://www.encodeproject.org'
SEARCH_URI = '/search/?type=File&field=uuid&field=file_format&field=lab.title&field=biosample_ontology.term_name&field=targets&field=target&field=assay_title&field=annotation_type&field=donors&field=award.rfa&field=status&field=preferred_default&field=analysis.title&format=json&limit='

def get_search_results(portal_auth: tuple, search_uri):
    response = requests.get(search_uri, auth=portal_auth)
    results = response.json()["@graph"]
    return results

def main(args):
    portal_auth = (args.portal_key, args.portal_secret_key)
    search = PORTAL_URI + SEARCH_URI + args.limit
    logging.info(f"getting results from {search}")
    results_json = get_search_results(portal_auth, search)
    logging.info(f"writing results to {args.output_filename}")
    with open(args.output_filename, "wt") as f:
        json.dump(results_json, f, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal-key", type=str)
    parser.add_argument("--portal-secret-key", type=str)
    parser.add_argument("--output-filename", type=str)
    parser.add_argument("--limit", type=str, default="all")
    args = parser.parse_args()
    main(args)


