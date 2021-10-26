"""Functions for running RDFox with necessary input files and scripts, and
collecting results.

This aims to hide the complexity of setting up RDFox, loading data, adding
rules, answering queries, behind a simple function that maps data -> answers.
"""


from contextlib import contextmanager
import logging
from typing import Dict, ContextManager, Iterator
from io import StringIO
import shutil

try:
    import importlib.resources
    DEFAULT_SCRIPT_SOURCE_DIR = importlib.resources.files("probs_ontology")
except (ImportError, AttributeError):
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources
    DEFAULT_SCRIPT_SOURCE_DIR = importlib_resources.files("probs_ontology")

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


def probs_convert_data(
    datasources, output_path, working_dir=None, script_source_dir=None
) -> None:
    """Run RDFox to load `facts` and `rules` then return answer from `queries`.

    :param datasources: list of Datasource
    :param output_path: path to save the data
    :param working_dir: Path to setup rdfox in, defaults to a temporary directory
    :param script_source_dir: Path to copy scripts from
    """

    if script_source_dir is None:
        # Use the version of the ontology scripts bundled with the Python
        # package
        script_source_dir = DEFAULT_SCRIPT_SOURCE_DIR

    # Standard files
    input_files = {
        "data/probs.fss": script_source_dir / "probs.fss",
        "data/additional_info.ttl": script_source_dir / "additional_info.ttl",
        "scripts/shared": script_source_dir / "scripts/shared",
        "scripts/data-conversion": script_source_dir / "scripts/data-conversion",
    }

    # Datasources -- the actual info
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

    script = [
        "exec scripts/data-conversion/master",
    ]

    with RDFoxRunner(input_files, script, NAMESPACES, working_dir=working_dir) as rdfox:
        shutil.copy(rdfox.files("data/probs_original_data.nt.gz"), output_path)

    # Should somehow signal success or failure


def probs_query_data(
    facts_path,
    queries,
    working_dir=None,
    script_source_dir=None,
    enhance=True,
) -> Dict:
    """Run RDFox to query pre-prepared facts.

    :param facts_path: Path to where facts have been written (in ".nt.gz" format)
    :param queries: Dict of {query_name: query_text}, or list of [query_text].
    :param working_dir: Path to setup rdfox in, defaults to a temporary directory
    :param script_source_dir: Path to copy scripts from
    :param enhance: whether to run "data-enhancement" stage first
    :return: Dict of {query_name: result}
    """
    if isinstance(queries, list):
        queries = {i: query_text for i, query_text in enumerate(queries)}
    elif not isinstance(queries, dict):
        raise ValueError("query should be list or dict")

    with probs_endpoint(facts_path, working_dir, script_source_dir, enhance) as rdfox:
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


@contextmanager
def probs_endpoint(
    facts_path,
    working_dir=None,
    script_source_dir=None,
    enhance=True,
) -> Iterator:
    """Run RDFox to query pre-prepared facts.

    :param facts_path: Path to where facts have been written (in ".nt.gz" format)
    :param working_dir: Path to setup rdfox in, defaults to a temporary directory
    :param script_source_dir: Path to copy scripts from
    :param enhance: whether to run "data-enhancement" stage first
    :return: Dict of {query_name: result}
    """

    if script_source_dir is None:
        # Use the version of the ontology scripts bundled with the Python
        # package
        script_source_dir = DEFAULT_SCRIPT_SOURCE_DIR

    # Standard files
    input_files = [
        "data/probs.fss": script_source_dir / "probs.fss",
        "data/additional_info.ttl": script_source_dir / "additional_info.ttl",
        "scripts/shared": script_source_dir / "scripts/shared",
        "scripts/data-enhancement": script_source_dir / "scripts/data-enhancement",
        "scripts/reasoning": script_source_dir / "scripts/reasoning",
    ]

    if enhance:
        # Add the given facts as data to be enhanced
        input_files["data/probs_original_data.nt.gz"] = facts_path

        script = [
            'set endpoint.port "12112"',
            "exec scripts/shared/setup-RDFox",
            # Missing from pipeline? Copied from scripts/data-enhancement/input.rdfox
            "exec scripts/shared/init-enhancement",
            "import probs.fss",
            "import additional_info.ttl",
            "import probs_original_data.nt.gz",
            # end copy
            "exec scripts/data-enhancement/master-pipeline",
            "exec scripts/reasoning/master-pipeline",
        ]

    else:
        # Add the given facts as already-enhanced data
        input_files["data/probs_enhanced_data.nt.gz"] = facts_path

        script = [
            'set endpoint.port "12112"',
            "exec scripts/reasoning/master",
        ]

    with RDFoxRunner(input_files, script, NAMESPACES, working_dir=working_dir, wait="endpoint") as rdfox:
        yield rdfox


def probs_convert_and_query_data(
    datasources, queries, print_facts=False, working_dir=None, script_source_dir=None
) -> Dict:
    """Run RDFox to load `facts` and `rules` then return answer from `queries`.

    :param facts: Facts in ttl format (string)
    :param rules: Facts in datalog format (string)
    :param queries: Dict of {query_name: query_text}, or list of [query_text].
    :param print_facts: whether to dump all RDFox facts for debugging
    :param working_dir: Path to setup rdfox in, defaults to a temporary directory
    :param script_source_dir: Path to copy scripts from
    :return: Dict of {query_name: result}
    """
    if isinstance(queries, list):
        queries = {i: query_text for i, query_text in enumerate(queries)}
    elif not isinstance(queries, dict):
        raise ValueError("query should be list or dict")

    if script_source_dir is None:
        # Use the version of the ontology scripts bundled with the Python
        # package
        script_source_dir = DEFAULT_SCRIPT_SOURCE_DIR

    # Standard files
    input_files = {
        "data/probs.fss": script_source_dir / "probs.fss",
        "data/additional_info.ttl": script_source_dir / "additional_info.ttl",
        "scripts/shared": script_source_dir / "scripts/shared",
        "scripts/data-conversion": script_source_dir / "scripts/data-conversion",
        "scripts/data-enhancement": script_source_dir / "scripts/data-enhancement",
        "scripts/reasoning": script_source_dir / "scripts/reasoning",
    }

    # Datasources -- the actual info
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

    # Placeholder for data
    input_files["data/.placeholder"] = StringIO("")

    script = [
        'set endpoint.port "12112"',
        "exec scripts/shared/setup-RDFox",
        "exec scripts/data-conversion/master-pipeline",
        "exec scripts/data-enhancement/master-pipeline",
        "exec scripts/reasoning/master-pipeline",
    ]

    with RDFoxRunner(input_files, script, NAMESPACES, working_dir=working_dir, wait="endpoint") as rdfox:
        if print_facts:
            print()
            print("--- Dump of RDFox data: ---")
            print(rdfox.facts())

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


class ProbsFacts:
    def __init__(
        self, sources, print_facts=False, working_dir=None, script_source_dir=None
    ):
        if isinstance(sources, str):
            sources = [Datasource.from_facts(sources)]

        self.sources = sources
        self.print_facts = print_facts
        self.working_dir = working_dir
        self.script_source_dir = script_source_dir

    def query(self, query):
        if isinstance(query, str):
            query = [query]
            unwrap = True
        else:
            unwrap = False

        result = probs_convert_and_query_data(
            self.sources,
            query,
            self.print_facts,
            self.working_dir,
            self.script_source_dir,
        )

        return result[0] if unwrap else result

    def query_one(self, query):
        result = self.query(query)
        if len(result) != 1:
            raise ValueError(f"Expected only 1 result but got {len(result)}")
        return result[0]
