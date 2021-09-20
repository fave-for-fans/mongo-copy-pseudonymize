import json
from pymongo import MongoClient
from tqdm import tqdm
from functools import partial
from copy import deepcopy
import argparse


def read_config(config_filename):
    """Read the json config."""

    with open(config_filename, 'r') as f:
        config = json.load(f, strict=False)

    return config

def get_database_connection(config, section):
    """Get a pymongo connection."""

    client = MongoClient(config[section + '_server'])
    db = client[config[section + '_database']]
    return db


def iterate_mongo(db):
    """Go over each entry in a database."""
    for collection_name in tqdm(db.list_collection_names(), desc="DB"):
        collection = source[collection_name]
        for entry in tqdm(collection.find(), desc=collection_name,
                          total=collection.estimated_document_count()):
            yield collection_name, entry

def sink(g):
    """Iterate over all entries."""
    for entry in g:
        pass


def pseudonymize(collection, entry, pseudonymizer, config):
    """Pseudonymize an entry in the database."""

    entry_out = entry

    if collection in config['pseudonymize']:
        entry_out = deepcopy(entry)

        config_keys = config['pseudonymize'][collection]
        entry_keys = entry_out.keys()

        # print(config_keys, entry_keys)

        for field in set(config_keys).intersection(entry_keys):
            entry_out[field] = pseudonymizer.pseudonymize(entry[field])

    return collection, entry_out


def write_entry(collection, entry, db):
    """Write data to a collection."""
    db[collection].insert_one(entry)
    return collection, entry


def compose_with_args(fs):
    """Apply many functions one after another."""

    def compose_fcn(args):
        for f in fs:
            # print(f, args)
            args = f(*args)
        return args

    return compose_fcn


class CountPseudonymizer():
    """Pseudonymize strings."""
    
    def __init__(self):
        self.mapping = {}
    
    def pseudonymize(self, inp):
        inp = str(inp)
        
        if inp in self.mapping:
            return self.mapping[inp]
        
        out = str(len(self.mapping))
        
        self.mapping[inp] = out
        
        return out


parser = argparse.ArgumentParser(description="Copy a mongo database with pseudonymization")
parser.add_argument('--config', type=str, required=True)

if __name__ == '__main__':
    # obtaining config filename from command line
    args = parser.parse_args()
    config_filename = args.config

    # connecting to the source and to the destination
    config = read_config(config_filename)
    source = get_database_connection(config, "source")
    destination = get_database_connection(config, "destination")

    # function to pseudonymize entries
    pseudonymize_p = partial(pseudonymize,
                             pseudonymizer=CountPseudonymizer(),
                             config=config)

    # function to write to the target
    write_p = partial(write_entry, db=destination)

    # two functions together
    pseudonymize_and_write = compose_with_args([pseudonymize_p, write_p])

    # generator with all items in the source database
    items = iterate_mongo(source)

    # generator with entries after pseudonymizing and writing
    out = map(pseudonymize_and_write, items)

    # running the operation
    sink(out)
