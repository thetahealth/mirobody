"""
Pytest configuration for pulse format_data tests
"""


def pytest_addoption(parser):
    parser.addoption(
        "--update-snapshots",
        action="store_true",
        default=False,
        help="Update expected snapshots in fixture files",
    )
