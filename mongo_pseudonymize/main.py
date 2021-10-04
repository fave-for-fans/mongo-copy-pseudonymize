import json
from pymongo import MongoClient
from pymongo.errors import OperationFailure, DuplicateKeyError
from tqdm import tqdm
from functools import partial
from copy import deepcopy
import argparse


def read_config(config_filename):
    """Read the json config."""

    with open(config_filename, 'r') as f:
        config = json.load(f, strict=False)

    return config

def get_database_connection(config, section, drop=False):
    """Get a pymongo connection."""


    server = config[section + "_server"]
    database = config[section + "_database"]

    print(f"[ .. ] Connecting to {server} database {database}")
    client = MongoClient(server)

    if drop:
        print(f"Dropping database {database} on {server}")
        client.drop_database(database)

    db = client[database]

    print(f"[ OK ] Connected  to {server} database {database}")

    return db


def iterate_mongo(db):
    """Go over each entry in a database."""
    for collection_name in tqdm(db.list_collection_names(), desc="DB"):
        collection = source[collection_name]
        try:
            for entry in tqdm(collection.find(), desc=collection_name,
                              total=collection.estimated_document_count()):
                yield collection_name, entry
        except OperationFailure as e:
            print("Copy failed for collection " + collection_name)
            print(e)

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
    try:
        db[collection].insert_one(entry)
    except DuplicateKeyError:
        pass
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
parser.add_argument('--drop', action='store_true')


def main_fcn():
    """Main function to run."""

    # obtaining config filename from command line
    args = parser.parse_args()

    # connecting to the source and to the destination
    config = read_config(args.config)
    source = get_database_connection(config, "source")
    destination = get_database_connection(config, "destination",
                                          drop=args.drop)

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
