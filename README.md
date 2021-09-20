# mongo-copy-pseudonymize

Copy data from one database to another with pseudonymization

1. Install miniconda
2. `conda env create -f environment.yml`
3. `conda activate mongo-copy-pseudonymize`
4. Edit your config file `config.json` (see `config.json.template`)
5. Run `python mongo_pseudonymize/main.py --config config.json`. The `--drop` argument removes the target database
