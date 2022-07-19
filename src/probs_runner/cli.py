"""Command-line tool for probs_runner."""

import time
import urllib.parse
import pathlib
import logging
import click
from rdflib import URIRef
from rdflib.namespace import RDF, RDFS

from .namespace import PROBS
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
@click.argument("output", nargs=1, type=click.Path(path_type=pathlib.Path))
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


INSPECT_QUERY = """
SELECT ?p ?o ?label
WHERE {
    ?s ?p ?o .
    OPTIONAL { ?o rdfs:label ?label }
}
ORDER BY ?p ?o
"""

INSPECT_OBSERVATIONS_COUNT = """
SELECT (COUNT(DISTINCT ?Observation) AS ?count) (SUM(COALESCE(?DirectCounter, 0)) AS ?direct)
WHERE {
  ?Observation a :Observation .
  OPTIONAL { ?Observation a :DirectObservation. BIND(1 AS ?DirectCounter) }
}
"""

INSPECT_OBSERVATIONS_ID = """
SELECT ?Observation WHERE { ?Observation a :Observation }
"""

INSPECT_OBSERVATIONS_SUMMARY = """
SELECT ?p (COUNT(DISTINCT ?o) AS ?count) (GROUP_CONCAT(DISTINCT STR(?o)) AS ?values)
WHERE {
  ?Observation a :Observation; ?p ?o .
  FILTER(?p IN (:processDefinedBy, :objectDefinedBy, :hasRegion, :hasTimePeriod, :hasRole, :metric))
}
GROUP BY ?p
ORDER BY ?p
"""


def _inspect(rdfox, subject):
    result = rdfox.query_records(INSPECT_QUERY,
                                n3=True,
                                initBindings={"s": URIRef(subject)})

    if not result:
        print("** Nothing found!")
        return

    values = {}
    for x in result:
        values.setdefault(x["p"], set())
        values[x["p"]] |= {x["o"]}

    # if PROBS.Observation in values[RDF.type]:
    #     print("OBSERVATION")
    for p, vals in values.items():
        print(p)
        for v in vals:
            print("  ", v)
        print()


@cli.command()
@click.argument("inputs", nargs=-1, type=click.Path(exists=True, path_type=pathlib.Path))
@click.option(
    "-s",
    "--subject",
    multiple=True,
)
@click.option(
    "--summary",
    help="Print a summary of observations found in the data.",
    is_flag=True,
)
@click.option(
    "--format",
    help="Output format (Graphviz or plain text).",
    type=click.Choice(['text', 'graphviz'], case_sensitive=False)
)
# @click.option(
#     "-l",
#     "--load",
#     type=click.Path(exists=True, path_type=pathlib.Path),
#     multiple=True,
# )
@click.pass_obj
def inspect(obj, inputs, subject, summary, format):
    "Load facts and inspect a PRObs subject."
    click.echo("Loading data...", err=True)

    script_source_dir = obj["script_source_dir"]

    with probs_endpoint(inputs,
                        port=12130,
                        script_source_dir=script_source_dir) as rdfox:

        if summary and format == "text":
            print()
            result = rdfox.query_records(INSPECT_OBSERVATIONS_COUNT)
            for row in result:
                print("{count:3d} Observations, of which {direct:3d} are Direct.".format(**row))
            print()
            result = rdfox.query_records(INSPECT_OBSERVATIONS_SUMMARY, n3=True)
            for row in result:
                print("{p:40s} {count:3d}".format(**row))

        elif summary and format == "graphviz":
            result = rdfox.query_records(INSPECT_OBSERVATIONS_ID)
            print("digraph G {")
            for row in result:
                _inspect_observation_graphviz(rdfox, row["Observation"])
            print("}")

        elif subject:
            for s in subject:
                _inspect(rdfox, s)

        else:
            while True:
                subject = input("Subject> ")
                if not subject:
                    break
                _inspect(rdfox, subject)


def _inspect_observation_graphviz(rdfox, subject):
    result = rdfox.query_records(INSPECT_QUERY,
                                 n3=True,
                                 initBindings={"s": URIRef(subject)})

    if not result:
        return

    if subject.startswith(PROBS):
        subject = "probs:" + subject[len(PROBS):]
    else:
        subject = f"<{subject}>"

    values = {}
    labels = {}
    for x in result:
        values.setdefault(x["p"], set())
        values[x["p"]] |= {x["o"]}
        if x["label"] is not None:
            labels[x["o"]] = x["label"]

    # print("  # Labels")
    # for k, v in labels.items():
    #     print(f'  "{k}" [label="{v}"];')

    label = []
    value = ""
    label_values = [
        "probs:hasRegion",
        "probs:hasTimePeriod",
        "probs:hasRole",
    ]
    for p in label_values:
        label.append(", ".join(values[p]))
    if "probs:bound" in values:
        vals = values["probs:bound"]
        value = ("== " if "probs:ExactBound" in vals else "&gt;") + value
    if "probs:measurement" in values:
        vals = values["probs:measurement"]
        value += str(list(vals)[0])


    def _label(x):
        if x in labels:
            return labels[x]
        else:
            _, _, last_part = x.rpartition("/")
            return last_part.rstrip(">")

    extra_label = []
    extra_label.append(", ".join(
        [f"{_label(x)}" for x in values.get("probs:objectDirectlyDefinedBy", [])] +
        [f"({_label(x)})" for x in values.get("probs:objectInferredDefinedBy", [])]
    ))
    extra_label.append(", ".join(
        [f"{_label(x)}" for x in values.get("probs:processDirectlyDefinedBy", [])] +
        [f"({_label(x)})" for x in values.get("probs:processInferredDefinedBy", [])]
    ))

    if subject.startswith("<http://ukfires.org/probs/data/"):
        subject_label = subject[len("<http://ukfires.org/probs/data/"):].strip("<>")
    else:
        subject_label = subject.strip("<>")


    label = (
        '{' +
        f'{subject_label[:50]} | ' +
        '{' + " | ".join(label + [value]) + '} | ' +
        '{' + " | ".join(extra_label) + '}' +
        '}'
    )
    print(f'  "{subject}" [shape=record,label="{label}"];')

    edge_values = [
        "prov:wasDerivedFrom",
        # "probs:objectDirectlyDefinedBy",
        # "probs:objectInferredDefinedBy",
        # "probs:processDirectlyDefinedBy",
        # "probs:processInferredDefinedBy",
    ]
    for p in edge_values:
        if p not in values:
            continue
        for v in values[p]:
            if p == "prov:wasDerivedFrom":
                # backwards meaning/flow
                print(f'  "{v}" -> "{subject}" [dir=back, color=gray, label="{p}"]')
            else:
                if "Directly" in p:
                    attrs = "penwidth=1.5, "
                elif "Inferred" in p:
                    attrs = "style=dashed, "
                print(f'  "{subject}" -> "{v}" [{attrs}label="{p}"]')
