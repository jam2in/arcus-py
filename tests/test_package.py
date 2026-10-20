from importlib.metadata import version

import arcus


def test_version_matches_distribution_metadata() -> None:
    assert arcus.__version__ == version("arcus-py")
