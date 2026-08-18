"""The config-file fan-out, which used to be unreachable from a test.

These cases lived inside `Config.init` — an async staticmethod that also sets
up logging and fetches remote config over HTTP — so the only way to exercise
the ordering rules was to boot the app and look at what it loaded.
"""

from __future__ import annotations

from .yaml_files import expand_yaml_filenames


def test_each_yaml_brings_its_key_sibling():
    """The `.key.yaml` split is what keeps secrets out of git; it must be
    automatic, and must land AFTER its base file so it wins."""
    assert expand_yaml_filenames("config.yaml", env="") == [
        "config.yaml",
        "config.key.yaml",
    ]


def test_a_key_file_has_no_sibling_of_its_own():
    """`config.key.yaml` must not expand to `config.key.key.yaml`."""
    assert expand_yaml_filenames("config.key.yaml", env="") == ["config.key.yaml"]


def test_env_fans_each_name_out_to_its_variant():
    """Order is the contract: base, then its secrets, then the env override,
    then the env secrets — each later file overriding the one before."""
    assert expand_yaml_filenames("config.yaml", env="prod") == [
        "config.yaml",
        "config.prod.yaml",
        "config.key.yaml",
        "config.prod.key.yaml",
    ]


def test_no_filenames_with_env_falls_back_to_the_convention():
    assert expand_yaml_filenames(None, env="dev") == [
        "config.dev.yaml",
        "config.dev.key.yaml",
    ]


def test_no_filenames_and_no_env_asks_for_nothing():
    """`Config.init` adds `config.yaml` itself when it exists on disk, so an
    empty request must stay empty rather than guessing a name here."""
    assert expand_yaml_filenames(None, env="") == []
    assert expand_yaml_filenames([], env="") == []


def test_duplicates_are_collapsed_not_loaded_twice():
    assert expand_yaml_filenames(["config.yaml", "config.yaml"], env="") == [
        "config.yaml",
        "config.key.yaml",
    ]


def test_non_yaml_entries_are_ignored_rather_than_fatal():
    """Historical behaviour: a stray entry must not stop the process booting."""
    assert expand_yaml_filenames(["notes.txt", "config.yaml"], env="") == [
        "notes.txt",
        "config.yaml",
        "config.key.yaml",
    ]


def test_non_string_entries_are_skipped():
    assert expand_yaml_filenames(["config.yaml", None, 42], env="") == [
        "config.yaml",
        "config.key.yaml",
    ]


def test_multiple_files_keep_their_relative_order():
    assert expand_yaml_filenames(["a.yaml", "b.yaml"], env="stg") == [
        "a.yaml",
        "a.stg.yaml",
        "a.key.yaml",
        "a.stg.key.yaml",
        "b.yaml",
        "b.stg.yaml",
        "b.key.yaml",
        "b.stg.key.yaml",
    ]
