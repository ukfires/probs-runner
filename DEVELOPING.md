# Developing probs_runner

## Repository structure

- `docs` contains the documentation for the package.
- `src/probs_runner` contains Python code for setting up RDFox scripts.
- `tests` contains the tests.

## Documentation

Documentation is written using [jupyter-book](https://jupyterbook.org).

Install the Python package in a virtual environment with the necessary dependencies:

```shell
pip install -e '.[docs]'
```

Build the docs:

```shell
jupyter-book build docs
```

## Releases

To release a new version of the package:

- Ensure tests are passing, documentation and changelog is up to date.

- Bump the version number according to [semantic versioning](https://semver.org/), based on the type of changes made since the last release.

- Commit the new version and tag a release like "v0.1.2"

- Build the package: `python setup.py sdist bdist_wheel`

- Publish the package to PyPI: `twine upload dist/probs_runner-[...]`

## Tests

Install the Python package in a virtual environment:

```shell
pip install -e '.[test]'
```

Run the tests using pytest:

```shell
pytest
```

See [tests/README.md](tests/README.md) for more details of how to use the test runner.

### Choosing the ontology scripts to use

The environment variable `PROBS_SCRIPT_SOURCE_DIR` can be set to specify the script source directory. For example:

```shell
PROBS_SCRIPT_SOURCE_DIR=/path/to/ontology/scripts pytest
```
