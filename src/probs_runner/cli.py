"""Command-line tool for probs_runner."""

import time
import urllib.parse
import pathlib
import logging
import click

from .runners import NAMESPACES, probs_convert_data, probs_validate_data, probs_enhance_data, probs_endpoint
from .datasource import load_datasource


LOG_LEVELS = {
    1: logging.INFO,
    2: logging.DEBUG,
}

@click.group()
@click.version_option()
@click.option('-v', '--verbose', count=True)
@click.option(
    "-s",
    "--scripts",
    help="PRObs ontology folder",
    type=click.Path(file_okay=False, exists=True, readable=True,
                    path_type=pathlib.Path),
)
@click.option(
    "-w",
    "--working-dir",
    help="Working directory",
    type=click.Path(file_okay=False,
                    path_type=pathlib.Path),
)
@click.pass_context
def cli(ctx, verbose, scripts, working_dir):
    """Command-line tool for probs-runner"""

    if verbose:
        level = LOG_LEVELS.get(verbose, logging.DEBUG)
        logger = logging.getLogger("probs_runner")
        logger.setLevel(level)
        logging.basicConfig()
        logging.getLogger("rdfox_runner").setLevel(level)
        logger.info("Set logging level %s", level)

    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['script_source_dir'] = scripts
    ctx.obj['working_dir'] = working_dir


@cli.command()
@click.argument("inputs", nargs=-1, type=click.Path(exists=True, path_type=pathlib.Path))
@click.argument("output", nargs=1, type=click.Path(exists=True, path_type=pathlib.Path))
@click.pass_obj
def convert_data(obj, inputs, output):
    "Convert input data into PRObs RDF format."

    click.echo(f"Converting {len(inputs)} inputs...", err=True)

    # Load data sources
    datasources = [load_datasource(path) for path in inputs]
    working_dir = obj["working_dir"]
    script_source_dir = obj["script_source_dir"]
    probs_convert_data(datasources, output, working_dir, script_source_dir)

    click.echo(f"Output written to {click.format_filename(output)}.", err=True)


@cli.command()
@click.argument("inputs", nargs=-1, type=click.Path(exists=True, path_type=pathlib.Path))
@click.pass_obj
def validate_data(obj, inputs):
    "Validate converted RDF data."

    click.echo(f"Checking {len(inputs)} inputs...", err=True)
    click.echo(f"(make sure to run with '-vv' option to see the results)", err=True)

    # Load data sources
    working_dir = obj["working_dir"]
    script_source_dir = obj["script_source_dir"]
    probs_validate_data(inputs, working_dir, script_source_dir)

    # TODO: do something with the validation result. Currently you need to run
    # this with debug output.


@cli.command()
@click.argument("inputs", nargs=-1, type=click.Path(exists=True, path_type=pathlib.Path))
@click.argument("output", nargs=1, type=click.Path(exists=True, path_type=pathlib.Path))
@click.pass_obj
def enhance_data(obj, inputs, output):
    "Run enhancement scripts on PRObs RDF data."

    click.echo(f"Enhancing {len(inputs)} inputs...", err=True)

    # Load data sources
    working_dir = obj["working_dir"]
    script_source_dir = obj["script_source_dir"]
    probs_enhance_data(inputs, output, working_dir, script_source_dir)

    click.echo(f"Output written to {click.format_filename(output)}.", err=True)


DEFAULT_QUERY = """
SELECT ?Observation ?p ?o
WHERE {
    ?Observation a :Observation; ?p ?o .
}
ORDER BY ?Observation ?p ?o
"""


def _default_query():
    """Define some useful prefixes and a test query."""
    prefixes = [
        f"PREFIX {p}: <{v}>"
        for p, v in NAMESPACES.items()
    ]
    content = "\n".join(prefixes) + "\n" + DEFAULT_QUERY
    return content


@cli.command()
@click.argument("inputs", nargs=-1, type=click.Path(exists=True, path_type=pathlib.Path))
@click.option(
    "-p",
    "--port",
    help="RDFox endpoint port",
    type=click.INT,
)
@click.option(
    "-q",
    "--query",
    "query_files",  # Python argument name
    help="File to load query(s) from",
    type=click.File("r"),
    multiple=True,
)
@click.option(
    "--console/--no-console",
    help="Whether to launch endpoint console",
    default=True,
)
@click.pass_obj
def endpoint(obj, inputs, port, query_files, console):
    "Start an RDFox endpoint based on DATA_PATH"
    click.echo("Starting endpoint...", err=True)

    queries = [f.read() for f in query_files]
    if not queries:
        queries = [_default_query()]
    query = urllib.parse.quote("\n".join(queries))

    script_source_dir = obj["script_source_dir"]

    with probs_endpoint(inputs,
                        port=port,
                        script_source_dir=script_source_dir) as rdfox:

        url = f"{rdfox.server}/console/default?query={query}"
        click.echo("Started endpoint", err=True)
        if console:
            click.launch(url)
        click.echo(f"Open {url} in your browser.", err=True)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            click.echo("Stopping endpoint")
