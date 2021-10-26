# -*- coding: utf-8 -*-

from pathlib import Path
import gzip
import pytest

from rdflib import Namespace, Graph, Literal
from rdflib.namespace import RDF
from probs_runner import (
    ProbsFacts,
    PROBS,
    Datasource,
    load_datasource,
    probs_convert_and_query_data,
    probs_convert_data,
    probs_query_data,
    probs_endpoint,
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
    result = probs_convert_and_query_data(
        [source],
        ["SELECT ?p ?o WHERE { :Farming ?p ?o }"],
        working_dir=tmp_path,
        script_source_dir=SCRIPT_SOURCE_DIR,
    )[0]
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


def test_query_data(tmp_path):
    source = load_datasource(Path(__file__).parent / "sample_datasource_simple")
    output_filename = tmp_path / "output.nt.gz"
    probs_convert_data(
        [source],
        output_filename,
        working_dir=tmp_path / "working_conversion",
        script_source_dir=SCRIPT_SOURCE_DIR,
    )

    # Now query the converted data
    query = "SELECT ?obj ?value WHERE { ?obj :hasValue ?value } ORDER BY ?obj"
    result = probs_query_data(
        output_filename,
        [query],
        working_dir=tmp_path / "working_reasoning",
        script_source_dir=SCRIPT_SOURCE_DIR,
    )[0]

    assert result == [
        {"obj": NS["Object-Bread"], "value": 6.0},
        {"obj": NS["Object-Cake"], "value": 3.0},
    ]


def test_probs_endpoint(tmp_path):
    source = load_datasource(Path(__file__).parent / "sample_datasource_simple")
    output_filename = tmp_path / "output.nt.gz"
    probs_convert_data(
        [source],
        output_filename,
        working_dir=tmp_path / "working_conversion",
        script_source_dir=SCRIPT_SOURCE_DIR,
    )

    # Now query the converted data
    query = "SELECT ?obj ?value WHERE { ?obj :hasValue ?value } ORDER BY ?obj"

    with probs_endpoint(
        output_filename,
        working_dir=tmp_path / "working_reasoning",
        script_source_dir=SCRIPT_SOURCE_DIR,
    ) as rdfox:
        result = rdfox.query_records(query)

    assert result == [
        {"obj": NS["Object-Bread"], "value": 6.0},
        {"obj": NS["Object-Cake"], "value": 3.0},
    ]


def test_query_from_datasource_folder(tmp_path):
    source = load_datasource(Path(__file__).parent / "sample_datasource_simple")
    result = probs_convert_and_query_data(
        [source],
        ["SELECT ?obj ?value WHERE { ?obj :hasValue ?value } ORDER BY ?obj"],
        working_dir=tmp_path,
        script_source_dir=SCRIPT_SOURCE_DIR,
    )[0]
    assert result == [
        {"obj": NS["Object-Bread"], "value": 6.0},
        {"obj": NS["Object-Cake"], "value": 3.0},
    ]


def test_probs_facts():
    facts = ProbsFacts(r""":Farming a :Process ; :name "Farming" .""",
                       script_source_dir=SCRIPT_SOURCE_DIR)
    result = facts.query_one(
        r"""SELECT ?type ?name WHERE { :Farming a ?type ; :name ?name }"""
    )
    assert result["type"] == PROBS["Process"]
    assert result["name"] == "Farming"
