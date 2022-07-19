# -*- coding: utf-8 -*-

import re
from pathlib import Path
import pytest

from probs_runner import Datasource, load_datasource


class TestDatasourceCreatedManually:

    @pytest.fixture
    def datasource(self):
        return Datasource.from_facts(":Farming a :Process .")

    def test_has_load_data(self, datasource):
        match = re.match(r"import (.*)", datasource.load_data_script)
        assert match
        filename = f"data/{match.group(1)}"
        assert filename in datasource.input_files
        # check contents
        contents = datasource.input_files[filename].read()
        assert contents == ":Farming a :Process ."

    def test_has_no_rules(self, datasource):
        assert datasource.rules == ""


class TestDatasourceFromFolder:
    DATASOURCE_FOLDER = Path(__file__).parent / "sample_datasource_simple"

    @pytest.fixture
    def datasource(self):
        return load_datasource(self.DATASOURCE_FOLDER)

    def test_has_load_data(self, datasource):
        assert datasource.load_data_script.startswith("prefix ufrd:")

    def test_has_rules(self, datasource):
        assert datasource.rules.startswith(":Object[?ObjectID]")

    def test_has_input_files(self, datasource):
        assert datasource.input_files == {
            "data/sample_datasource_simple/data.csv": self.DATASOURCE_FOLDER / "data.csv"
        }


class TestDatasourceFromFilesCustomLoadDataScript:
    DATASOURCE_FOLDER = Path(__file__).parent / "sample_datasource_simple"

    def test_raises_error_for_csv_without_load_script(self):
        with pytest.raises(ValueError, match=r"cannot automatically load \{'\.csv'\}"):
            Datasource.from_files([self.DATASOURCE_FOLDER / "data.csv"])

    def test_loads_csv_with_load_script(self):
        input_file = self.DATASOURCE_FOLDER / "data.csv"
        load_data_script = self.DATASOURCE_FOLDER / "load_data.rdfox"
        ds = Datasource.from_files([input_file], load_data_script)

        keys = list(ds.input_files.keys())
        assert len(keys) == 1
        target_path = keys[0]
        assert target_path.name == "data.csv"
        assert ds.input_files[target_path] == input_file

        assert ds.load_data_script.endswith(load_data_script.read_text())

    def test_renames_input_files_with_dict(self):
        input_file = self.DATASOURCE_FOLDER / "data.csv"
        load_data_script = self.DATASOURCE_FOLDER / "load_data.rdfox"
        ds = Datasource.from_files({"something_else.csv": input_file},
                                   load_data_script)

        keys = list(ds.input_files.keys())
        assert len(keys) == 1
        target_path = keys[0]
        assert target_path.name == "something_else.csv"
        assert len(str(target_path)) > len("something_else.csv") + 10  # enough for hash
        assert ds.input_files[target_path] == input_file

    def test_automatically_loads_ttl(self, tmp_path):
        p = tmp_path / "data.ttl"
        p.write_text(":Farming a :Process .\n")
        ds = Datasource.from_files([p])
        assert 'import "$(dir.datasource)/data.ttl"' in ds.load_data_script
        assert ds.rules == ""


def test_datasource_from_files_accepts_str():
    a = Datasource.from_files(["a.ttl"])
    b = Datasource.from_files([Path("a.ttl")])
    assert a == b


def test_datasource_target_paths_are_unique():
    a = Datasource.from_files(["path1/x.ttl"])
    b = Datasource.from_files(["path2/x.ttl"])
    assert a.input_files != b.input_files


def test_datasource_reuses_same_path_with_different_load_data_scripts():
    a = Datasource.from_files(["a.csv"], load_data_script="a")
    b = Datasource.from_files(["a.csv"], load_data_script="b")
    assert a.input_files == b.input_files
    assert a != b


def test_datasource_reuses_same_path_with_different_rules():
    a = Datasource.from_files(["a.ttl"], rules="a")
    b = Datasource.from_files(["a.ttl"], rules="b")
    assert a.input_files == b.input_files
    assert a != b
def test_datasource_errors_for_missing_folder():
    with pytest.raises(NotADirectoryError):
        load_datasource(Path("MISSING_FOLDER"))
