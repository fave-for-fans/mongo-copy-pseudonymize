# mongo-copy-pseudonymize

Copy data from one MongoDB database to another with pseudonymization

The usual setup is to have a local MongoDB database (see [official documentation](https://docs.mongodb.com/manual/installation/) for installation instructions, and [Compass](https://www.mongodb.com/products/compass) for visual inspection).
The script reads the connection parameters (`source` and `destination` database connections) from a `.json` file, and then copies the entries from all collections. Additionally, it performs pseudonymization to hide sensitive information. The configuration is done in the `.json` file. Pseudonymization in the current version simply replaces selected fields with strings mapping 1-to-1.

1. Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
2. Create the environment: `conda env create -f environment.yml`
3. Activate the new environment: `conda activate mongo-copy-pseudonymize`
4. Edit your config file `config.json` (see `config.json.template`). The `source_server` is the MongoDB connection string for the original database, and `source_database` is the database name. `destination_server` and `destination_database` have the same format and define the target database. Please note that the `--drop` flag will erase the destination database! `pseudonymize` is a dictionary mapping colections to lists of fields to pseudonymize. 
5. Run `python mongo_pseudonymize/main.py --config config.json`. The `--drop` argument removes the target database.
