"""Functions for running RDFox with necessary input files and scripts, and
collecting results.

This aims to hide the complexity of setting up RDFox, loading data, adding
rules, answering queries, behind simple functions that map data -> answers.

The pipeline has multiple steps:
- The starting point are datasources (csv/loading rules)
- *Conversion* maps datasources to probs_original_data.nt.gz
- *Enhancement* maps probs_original_data.nt.gz to probs_enhanced_data.nt.gz
- We then start a *reasoning* endpoint to query the data

These correspond to the "basic" functions:
probs_convert_data(datasources) -> probs_original_data
probs_enhance_data(probs_original_data) -> probs_enhanced_data
probs_endpoint(probs_enhanced_data) -> endpoint

All of these functions accept some common options:
- working_dir
- script_source_dir

"""


from contextlib import contextmanager
import logging
from typing import Dict, ContextManager, Iterator
from io import StringIO
import shutil
from pathlib import Path
from tempfile import mkdtemp

try:
    import importlib.resources as importlib_resources
except (ImportError, AttributeError):
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources

import pandas as pd
from rdflib import Namespace
from rdflib.namespace import RDF, RDFS

from rdfox_runner import RDFoxRunner

from .datasource import Datasource
from .namespace import PROBS

logger = logging.getLogger(__name__)


NAMESPACES = {
    "sys": Namespace("https://ukfires.org/probs/system/"),
    "": PROBS,
    "rdf": RDF,
    "rdfs": RDFS,
}


DEFAULT_PORT = 12112

def _standard_input_files(script_source_dir):
    if script_source_dir is None:
        # Use the version of the ontology scripts bundled with the Python
        # package
        try:
            script_source_dir = importlib_resources.files("probs_ontology")
        except ModuleNotFoundError:
            raise RuntimeError("The probs_ontology package is not installed, and no script_source_dir has been specified.")

    # Standard files
    input_files = {
        "data/probs.fss": script_source_dir / "data/probs.fss",
        "data/additional_info.ttl": script_source_dir / "data/additional_info.ttl",
        "scripts/": script_source_dir / "scripts/",
        # "scripts/shared": script_source_dir / "scripts/shared",
        # "scripts/data-conversion": script_source_dir / "scripts/data-conversion",
    }

    return input_files


def _add_datasources_to_input_files(input_files, datasources):
    input_files["scripts/data-conversion/load_data.rdfox"] = StringIO(
        "\n".join(source.load_data_script for source in datasources)
    )
    input_files["scripts/data-conversion/map.dlog"] = StringIO(
        "\n".join(source.rules for source in datasources)
    )
    for datasource in datasources:
        for tgt, src in datasource.input_files.items():
            if tgt in input_files:
                raise ValueError(f"Duplicate entry in input_files for '{tgt}'")
            input_files[tgt] = src


def probs_convert_data(
    datasources, output_path, working_dir=None, script_source_dir=None
) -> None:
    """Load `datasources`, convert to RDF and copy result to `output_path`.

    :param datasources: list of Datasource
    :param output_path: path to save the data
    :param working_dir: Path to setup rdfox in, defaults to a temporary directory
    :param script_source_dir: Path to copy scripts from
    """

    input_files = _standard_input_files(script_source_dir)
    _add_datasources_to_input_files(input_files, datasources)
    script = ["exec scripts/data-conversion/master"]

    with RDFoxRunner(input_files, script, NAMESPACES, working_dir=working_dir) as rdfox:
        shutil.copy(rdfox.files("data/probs_original_data.nt.gz"), output_path)

    # Should somehow signal success or failure


def probs_enhance_data(
    original_data_path, output_path, working_dir=None, script_source_dir=None
) -> None:
    """Load `original_data_path`, apply rules to enhance, and copy result to `output_path`.

    :param original_data_path: path to probs_original_data.nt.gz
    :param output_path: path to save the data
    :param working_dir: Path to setup rdfox in, defaults to a temporary directory
    :param script_source_dir: Path to copy scripts from
    """

    input_files = _standard_input_files(script_source_dir)
    input_files["data/probs_original_data.nt.gz"] = original_data_path
    script = ["exec scripts/data-enhancement/master"]

    with RDFoxRunner(input_files, script, NAMESPACES, working_dir=working_dir) as rdfox:
        shutil.copy(rdfox.files("data/probs_enhanced_data.nt.gz"), output_path)

    # Should somehow signal success or failure


@contextmanager
def probs_endpoint(enhanced_data_path, working_dir=None, script_source_dir=None, port=DEFAULT_PORT) -> Iterator:
    """Load `enhanced_data_path`, and start endpoint.

    :param enhanced_data_path: path to probs_original_data.nt.gz
    :param working_dir: Path to setup rdfox in, defaults to a temporary directory
    :param script_source_dir: Path to copy scripts from
    :param port: Port number to listen on
    """

    if port is None:
        port = DEFAULT_PORT

    input_files = _standard_input_files(script_source_dir)
    input_files["data/probs_enhanced_data.nt.gz"] = enhanced_data_path

    script = [
        f'set endpoint.port "{int(port)}"',
        "exec scripts/reasoning/master",
    ]

    with RDFoxRunner(input_files, script, NAMESPACES, working_dir=working_dir, wait="endpoint") as rdfox:
        yield rdfox


def answer_queries(rdfox, queries) -> Dict:
    """Answer queries from RDFox endpoint.

    :param rdfox: RDFox endpoint
    :param queries: Dict of {query_name: query_text}, or list of [query_text].
    :return: Dict of {query_name: result}
    """
    if isinstance(queries, list):
        queries = {i: query_text for i, query_text in enumerate(queries)}
    elif not isinstance(queries, dict):
        raise ValueError("query should be list or dict")

    answers_df = {
        query_name: rdfox.query_records(query_text)
        for query_name, query_text in queries.items()
    }

    with pd.option_context(
        "display.max_rows", 100, "display.max_columns", 10, "display.max_colwidth", 200
    ):
        for k, v in answers_df.items():
            logger.info("Results from query %s:", k)
            logger.info("\n%s", pd.DataFrame.from_records(v))

    return answers_df

