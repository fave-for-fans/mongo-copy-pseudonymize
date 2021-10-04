# mongo-copy-pseudonymize

Copy data from one database to another with pseudonymization

1. Install miniconda
2. `conda env create -f environment.yml`
3. `conda activate mongo-copy-pseudonymize`
4. Edit your config file `config.json` (see `config.json.template`)
5. Run `python mongo_pseudonymize/__main__.py --config config.json`. The `--drop` argument removes the target database


## Installation
1. `pip install -e .`
2. `python -m mongo_pseudonymize`
