import click
import json
import os
import logging.config

from dotenv import load_dotenv
from barnacle.config import BarnacleConfig

CLI_DIR = os.path.dirname(os.path.realpath(__file__))
LOGGING_CONFIG = os.path.join(CLI_DIR, "logging.json")
if os.path.exists(LOGGING_CONFIG):
    with open(LOGGING_CONFIG, "rt") as fh:
        config = json.loads(fh.read())
        logging.config.dictConfig(config)


LOG = logging.getLogger(__name__)
@click.group()
def cli():
    pass


@cli.command()
def config():
    d = {k: v for k, v in dict(vars(BarnacleConfig)).items() if "__" not in k}
    click.echo(json.dumps(d, indent=True, sort_keys=True))

if __name__ == "__main__":
    cli(obj={})
