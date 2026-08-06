import click
import types
from unittest.mock import patch

from oceanum.__main__ import load_cli_extensions


class FakeEntryPoint:
    """Minimal stand-in for importlib.metadata.EntryPoint-like object."""
    def __init__(self, name, obj):
        self.name = name
        self._obj = obj

    def load(self):
        if isinstance(self._obj, Exception):
            raise self._obj
        return self._obj


def test_registrar_extension():
    """Registrar callable: load() returns a function that registers commands."""
    parent = click.Group("test-parent")

    def register_cli(parent_group):
        parent_group.add_command(click.Command("registered-cmd"), name="registered-cmd")

    ep = FakeEntryPoint("registrar-ext", register_cli)
    with patch("oceanum.__main__.entry_points", return_value=[ep]):
        load_cli_extensions(parent)
    assert "registered-cmd" in parent.commands


def test_broken_plugin_non_fatal(capsys):
    """Broken plugin raises exception but later plugins still load."""
    parent = click.Group("test-parent")

    class BrokenEP:
        name = "broken"
        def load(self):
            raise ImportError("missing dependency")

    good_group = click.Group(name="good")
    good_ep = FakeEntryPoint("good-ext", good_group)

    with patch("oceanum.__main__.entry_points", return_value=[BrokenEP(), good_ep]):
        load_cli_extensions(parent)

    captured = capsys.readouterr()
    assert "Error loading entry point broken" in captured.err
    assert "good" in parent.commands


def test_debug_safety(monkeypatch, capsys):
    """Debug mode with non-module target does not raise AttributeError."""
    monkeypatch.setenv("OCEANUM_CLI_DEBUG", "1")
    # Reload CLI_DEBUG after env change
    import importlib
    import oceanum.__main__ as m
    importlib.reload(m)
    from oceanum.__main__ import load_cli_extensions as lce

    parent = click.Group("test-parent")
    demo_group = click.Group(name="debug-demo")
    ep = FakeEntryPoint("debug-ext", demo_group)

    with patch("oceanum.__main__.entry_points", return_value=[ep]):
        lce(parent)

    captured = capsys.readouterr()
    assert "AttributeError" not in captured.out
    assert "AttributeError" not in captured.err
    assert "debug-demo" in parent.commands


def test_legacy_module_extension():
    """Legacy extension path: entry point returns a ModuleType, no commands added."""
    parent = click.Group("test-parent")
    ep = FakeEntryPoint("legacy-ext", types.ModuleType("legacy_module"))
    with patch("oceanum.__main__.entry_points", return_value=[ep]):
        load_cli_extensions(parent)
    assert len(parent.commands) == 0


def test_direct_command_extension():
    """Direct command extension path: entry point returns a click.Group, should be added."""
    parent = click.Group("test-parent")
    demo_group = click.Group(name="demo-group")
    ep = FakeEntryPoint("direct-ext", demo_group)
    with patch("oceanum.__main__.entry_points", return_value=[ep]):
        load_cli_extensions(parent)
    assert "demo-group" in parent.commands
