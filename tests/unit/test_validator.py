
import pytest

from edge_orchestrator.core.validator import (
    TaskValidationError,
    TaskValidator,
)


def test_valid_code_is_accepted():
    validator = TaskValidator()

    assert validator.validate("result = 10 + 20") is True


def test_function_code_is_accepted():
    validator = TaskValidator()

    code = """
def add(a, b):
    return a + b
"""

    assert validator.validate(code) is True


def test_empty_code_is_rejected():
    validator = TaskValidator()

    with pytest.raises(TaskValidationError):
        validator.validate("")


def test_whitespace_code_is_rejected():
    validator = TaskValidator()

    with pytest.raises(TaskValidationError):
        validator.validate("   ")


def test_invalid_syntax_is_rejected():
    validator = TaskValidator()

    with pytest.raises(TaskValidationError):
        validator.validate("if True print('hello')")


@pytest.mark.parametrize(
    "code",
    [
        "import os",
        "import subprocess",
        "import socket",
        "from sys import exit",
    ],
)
def test_disallowed_imports_are_rejected(code):
    validator = TaskValidator()

    with pytest.raises(TaskValidationError):
        validator.validate(code)


@pytest.mark.parametrize(
    "code",
    [
        "eval('1 + 1')",
        "exec('print(1)')",
        "compile('print(1)', '<string>', 'exec')",
        "__import__('os')",
    ],
)
def test_disallowed_calls_are_rejected(code):
    validator = TaskValidator()

    with pytest.raises(TaskValidationError):
        validator.validate(code)


def test_allowed_import_is_accepted():
    validator = TaskValidator()

    assert validator.validate("import math") is True
