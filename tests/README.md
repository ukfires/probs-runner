# pytest unit tests

The tests in this directory are unit tests based on [pytest](https://docs.pytest.org/en/stable/).

To run all the tests:

``` shell
pytest
```

To run a subset of tests use `-k` to filter, for example:
``` shell
pytest -k basic
```
runs only tests with "basic" in their filename, classname or test name.

## More information about failing tests

Show more detail including output from RDFox:
``` shell
pytest --log-level DEBUG
```
(other log levels such as INFO can also be used)

If the log level is set to at least INFO, it will print the temporary directory that was set up for RDFox to run. You can go to this path to check in more detail what's happening in the test.

## Other pytest options

Pytest has lots of convenient ways to choose the tests to run; see its documentation for more details.

Useful options include:
- `-x` to stop after the first failed test
- `--lf` to rerun only the tests that failed last time
- `--nf` to run newly added tests first
