"""The package carries the PEP 561 marker so consumers' type checkers read its annotations."""

import pathlib

import cocoonsandbox


def test_package_ships_the_pep561_marker():
    assert (pathlib.Path(cocoonsandbox.__file__).parent / "py.typed").is_file()
