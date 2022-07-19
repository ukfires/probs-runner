#!/usr/bin/env python3


from dataclasses import dataclass, field
from hashlib import md5
from pathlib import Path
from io import StringIO
from typing import Union, Optional


@dataclass
class Datasource:
    """Represent a set of input to `probs_convert_data`."""

    input_files: dict = field(default_factory=dict)
    load_data_script: str = ""
    rules: str = ""

    @classmethod
    def from_facts(cls, facts):
        """Create a datasource from explicit list of facts."""
        hash = md5(facts.encode()).hexdigest()
        input_files = {
            f"data/{hash}.ttl": StringIO(facts)
        }
        import_statement = f"import {hash}.ttl\n"
        return cls(input_files, import_statement)

    @classmethod
    def from_files(cls,
                   input_files: Union[dict, list],
                   load_data_script: Optional[Union[Path, str]] = None,
                   rules: Optional[Union[Path, str]] = None):
        """Load a `Datasource` from specified files.

        The keys of `input_files` are the filenames to be referred to in the
        `load_data_script`; the values are the paths to the source file.

        Alternatively, a `input_files` can be a list, in which case the name
        (without directory) of each path is used as the copied filename.

        In either case, the files specified in `input_files` are copied in to
        the RDFox working directory under a uniquely-named folder for the
        Datasource. This location is made available to the load_data script via
        the RDFox variable `dir.datasource`.

        The `load_data_script` passed as an argument is used if given.
        Otherwise, if all the paths to be loaded are .ttl files, they are loaded
        automatically. If none of these options has produced a load_data script,
        an error is raised.

        `load_data_script` and `rules` can be either a string, which is used
        literally, or a `pathlib.Path` to be read.

        """

        if isinstance(input_files, list):
            input_files_list = input_files
            input_files = {}
            for source_path in input_files_list:
                source_path = Path(source_path)
                if source_path.name in input_files:
                    raise ValueError("Duplicate path name in list; use dict to specify "
                                     "different names")
                input_files[source_path.name] = source_path

        # Generate a unique but stable name
        datasource_name = md5(b"".join(bytes(p) for p in input_files.values())).hexdigest()

        full_input_files = {
            (Path("data") / datasource_name / filename): source_path
            for filename, source_path in input_files.items()
        }

        if load_data_script is None:
            # Try to load automatically
            suffices = {p.suffix for p in full_input_files}
            auto_suffices = {".ttl", ".nt", ".nt.gz", ".ttl.gz"}
            if suffices == auto_suffices:
                load_data_script = "# Auto generated to load TTL files\n" + "\n".join([
                    f'import "$(dir.datasource)/{p}"' for p in input_files
                ])
            else:
                raise ValueError("No load_data_script given, and cannot automatically load {} files"
                                 .format(suffices - auto_suffices))
        elif isinstance(load_data_script, Path):
            load_data_script = load_data_script.read_text()

        # XXX This should be more general -- i.e. not in this function?
        dir_setup = f'set dir.datasource "$(dir.root)/data/{datasource_name}/"'
        load_data_script = dir_setup + "\n" + load_data_script

        if rules is None:
            rules = ""
        elif isinstance(rules, Path):
            rules = rules.read_text()

        return Datasource(full_input_files, load_data_script, rules)


def load_datasource(path: Path):
    """Load a `Datasource` from path."""

    if not path.is_dir():
        raise NotADirectoryError(path)

    load_data_path = path / "load_data.rdfox"
    if load_data_path.exists():
        load_data_script = load_data_path
    else:
        load_data_script = ""

    map_path = path / "map.dlog"
    if map_path.exists():
        rules = map_path
    else:
        rules = ""

    data_files = list(path.glob("*.csv")) + list(path.glob("*.ttl"))

    datasource = Datasource.from_files(data_files, load_data_script, rules)
    return datasource
