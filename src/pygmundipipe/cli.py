from typing import Annotated
import typer

cli = typer.Typer()


@cli.command()
def clean(
    input_file: Annotated[str, typer.Argument(help='Input file to clean')],
) -> None:
    from .clean import clean_data_main

    clean_data_main(input_file)


@cli.command()
def process() -> None:
    from .process_data import process_data

    process_data('config.yml')


@cli.command()
def augment() -> None:
    from .augment import process_yaml_files

    process_yaml_files()


@cli.command()
def tokencounter(
    input_file: Annotated[str, typer.Argument(help='Input file to count tokens')],
) -> None:
    from .tokencounter import tokencounter

    tokencounter(input_file)
