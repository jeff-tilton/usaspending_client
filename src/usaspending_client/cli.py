import click

from .data.make_dataset import make_data


@click.group()
def cli():
    pass


cli.add_command(make_data)


if __name__ == "__main__":
    cli()
