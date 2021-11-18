# -*- coding: utf-8 -*-

from pathlib import Path
import gzip
import pytest

from rdflib import Namespace, Graph, Literal
from rdflib.namespace import RDF
from probs_runner import (
    PROBS,
    Datasource,
    load_datasource,
    probs_convert_data,
    probs_convert_endpoint,
    probs_convert_enhance_data,
    probs_convert_enhance_endpoint,
    probs_enhance_data,
    probs_enhance_endpoint,
    probs_endpoint,
    answer_queries,
)

import os

if "PROBS_SCRIPT_SOURCE_DIR" in os.environ:
    SCRIPT_SOURCE_DIR = Path(os.environ["PROBS_SCRIPT_SOURCE_DIR"])
    if not SCRIPT_SOURCE_DIR.exists():
        raise FileNotFoundError(f"SCRIPT_SOURCE_DIR not found: '{SCRIPT_SOURCE_DIR}'")
else:
    SCRIPT_SOURCE_DIR = None


def test_very_simple_query(tmp_path):
    source = Datasource.from_facts(":Farming a :Process .")
    with probs_convert_endpoint([source], working_dir=tmp_path, script_source_dir=SCRIPT_SOURCE_DIR) as rdfox:
        result = answer_queries(rdfox, ["SELECT ?p ?o WHERE { :Farming ?p ?o }"])[0]
    assert result == [{"p": RDF.type, "o": PROBS.Process}]


NS = Namespace("https://ukfires.org/probs/ontology/data/simple/")


def test_convert_data(tmp_path):
    source = load_datasource(Path(__file__).parent / "sample_datasource_simple")
    output_filename = tmp_path / "output.nt.gz"
    probs_convert_data(
        [source],
        output_filename,
        working_dir=tmp_path / "working",
        script_source_dir=SCRIPT_SOURCE_DIR,
    )

    # Should check for success or failure

    result = Graph()
    with gzip.open(output_filename, "r") as f:
        result.parse(f, format="nt")

    # TODO: should make the test case use the proper ontology
    assert (NS["Object-Bread"], PROBS.hasValue, Literal(6.0)) in result


def test_convert_endpoint(tmp_path):
    source = load_datasource(Path(__file__).parent / "sample_datasource_simple")
    query = "SELECT ?obj ?value WHERE { ?obj :hasValue ?value } ORDER BY ?obj"
    with probs_convert_endpoint([source], working_dir=tmp_path, script_source_dir=SCRIPT_SOURCE_DIR) as rdfox:
        result = rdfox.query_records(query)

    assert result == [
        {"obj": NS["Object-Bread"], "value": 6.0},
        {"obj": NS["Object-Cake"], "value": 3.0},
    ]

    # Should check for success or failure


def test_convert_enhance_data(tmp_path):
    # Check that the enhanced data has more than the original... Perhaps could
    # be a more targeted check that it's been enhanced in the correct way, but
    # that's mostly tested in the ontology repo.

    source = load_datasource(Path(__file__).parent / "sample_datasource_simple")
    original_filename = tmp_path / "original.nt.gz"
    enhanced_filename = tmp_path / "enhanced.nt.gz"

    probs_convert_data(
        [source],
        original_filename,
        working_dir=tmp_path / "working_original",
        script_source_dir=SCRIPT_SOURCE_DIR,
    )

    probs_convert_enhance_data(
        [source],
        enhanced_filename,
        working_dir=tmp_path / "working_enhanced",
        script_source_dir=SCRIPT_SOURCE_DIR,
    )

    # Should check for success or failure

    with gzip.open(original_filename, "r") as f:
        original_len = len(f.read())
    with gzip.open(enhanced_filename, "r") as f:
        enhanced_len = len(f.read())

    assert enhanced_len > original_len


def test_convert_enhance_endpoint(tmp_path):
    source = load_datasource(Path(__file__).parent / "sample_datasource_simple")

    # This is a fact that is added by enhancement
    query = "SELECT ?obj WHERE { ?obj :objectEquivalentTo ?obj }"
    with probs_convert_enhance_endpoint([source], working_dir=tmp_path, script_source_dir=SCRIPT_SOURCE_DIR) as rdfox:
        result = rdfox.query_records(query)

    assert result

    # Should check for success or failure


def test_enhance_data(tmp_path):
    original_filename = tmp_path / "original.nt.gz"
    enhanced_filename = tmp_path / "enhanced.nt.gz"
    with gzip.open(original_filename, "wt") as f:
        f.write("""
<https://ukfires.org/probs/ontology/data/simple/Object-Bread> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://ukfires.org/probs/ontology/Object> .
        """)

    probs_enhance_data(
        original_filename,
        enhanced_filename,
        working_dir=tmp_path / "working_enhanced",
        script_source_dir=SCRIPT_SOURCE_DIR,
    )

    with gzip.open(enhanced_filename, "rt") as f:
        lines = f.readlines()

    # Check something has been added...
    assert len(lines) > 1


def test_enhance_endpoint(tmp_path):
    output_filename = tmp_path / "output.nt.gz"
    with gzip.open(output_filename, "wt") as f:
        f.write("""
<https://ukfires.org/probs/ontology/data/simple/Object-Bread> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://ukfires.org/probs/ontology/Object> .
        """)

    # This is a fact that is added by enhancement
    query = "SELECT ?obj WHERE { ?obj :objectEquivalentTo ?obj }"
    with probs_enhance_endpoint(output_filename, working_dir=tmp_path, script_source_dir=SCRIPT_SOURCE_DIR) as rdfox:
        result = rdfox.query_records(query)

    assert result


def test_probs_endpoint(tmp_path):
    output_filename = tmp_path / "output.nt.gz"
    with gzip.open(output_filename, "wt") as f:
        f.writelines([
            '<https://ukfires.org/probs/ontology/data/simple/Object-Bread> <https://ukfires.org/probs/ontology/hasValue> "6"^^<http://www.w3.org/2001/XMLSchema#double> .',
            '<https://ukfires.org/probs/ontology/data/simple/Object-Cake> <https://ukfires.org/probs/ontology/hasValue> "3"^^<http://www.w3.org/2001/XMLSchema#double> .',
        ])

    # Now query the converted data
    query = "SELECT ?obj ?value WHERE { ?obj :hasValue ?value } ORDER BY ?obj"
    with probs_endpoint(output_filename, working_dir=tmp_path / "working_reasoning", script_source_dir=SCRIPT_SOURCE_DIR) as rdfox:
        result = rdfox.query_records(query)

    assert result == [
        {"obj": NS["Object-Bread"], "value": 6.0},
        {"obj": NS["Object-Cake"], "value": 3.0},
    ]
