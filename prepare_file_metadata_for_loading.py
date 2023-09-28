import argparse
import json
from itertools import islice
import ijson


def parse_target_or_targets(item):
    '''files can have either target or targets or neither but fortunately not both.
    '''
    target = item.get('target') or item.get('targets')
    if target:
        if isinstance(target, list):
            labels = [target['label'] for target in target]
            result = ' '.join(labels)
        if isinstance(target, dict):
            result = target['label']
    else:
        result = ''
    return result


def parse_metadata_item(item: dict):
    content = {}
    content['file_format'] = {'S': item['file_format']}
    content['lab'] = {'S': item['lab']['title']}
    content['uuid'] = {'S': item['uuid']}
    content['biosample_term_name'] = {'S': item.get('biosample_ontology', {}).get('term_name', '')}
    content['annotation_type'] = {'S': item.get('annotation_type', '')}
    content['assay_title'] = {'S': item.get('assay_title', '')}
    content['status'] = {'S': item.get('status', '')}
    content['preferred_default'] = {'B': item.get('preferred_default', False)}
    content['analysis_title'] = {'S': item.get('analysis', {}).get('title', '')}
    content['donors'] = {'S': ' '.join(item.get('donors', []))}
    content['targets'] = {'S': parse_target_or_targets(item)}
    content['award_rfa'] = {'S': item.get('award', {}).get('rfa', '')}
    return {
        'PutRequest': {
            'Item' : content
        }
    }


def json_processor(instream, outstream):
    outstream.write('[\n')
    objects = ijson.items(instream, 'item')
    first = True
    for obj in objects:
        parsed_obj = parse_metadata_item(obj)
        if not first:
            outfile.write(',\n')
        else:
            first = False
        json.dump(parsed_obj, outstream, indent=4)
    outfile.write('\n]')

def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-json", type=str)
    parser.add_argument("--output-filename", type=str)
    args = parser.parse_args()

    with open(args.input_json, 'rb') as infile, open(args.output_filename, 'w') as outfile:
        json_processor(infile, outfile)


