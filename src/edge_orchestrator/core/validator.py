
import ast
from typing import ClassVar


class TaskValidationError(Exception):
    """Raised when submitted task code fails validation."""


class TaskValidator:
    """Performs static validation of submitted Python code."""

    DISALLOWED_IMPORTS: ClassVar[set[str]] = {
        "os",
        "subprocess",
        "sys",
        "socket",
        "ctypes",
        "multiprocessing",
    }

    DISALLOWED_CALLS: ClassVar[set[str]] = {
        "exec",
        "eval",
        "compile",
        "__import__",
    }

    def validate(self, code: str) -> bool:
        if not code or not code.strip():
            raise TaskValidationError("Task code cannot be empty")

        try:
            tree = ast.parse(code)
        except SyntaxError as exc:
            raise TaskValidationError(
                f"Invalid Python syntax: {exc.msg}"
            ) from exc

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_module = alias.name.split(".")[0]
                    if root_module in self.DISALLOWED_IMPORTS:
                        raise TaskValidationError(
                            f"Import of '{root_module}' is not allowed"
                        )

            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    root_module = node.module.split(".")[0]
                    if root_module in self.DISALLOWED_IMPORTS:
                        raise TaskValidationError(
                            f"Import from '{root_module}' is not allowed"
                        )

            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in self.DISALLOWED_CALLS
            ):
                raise TaskValidationError(
                    f"Call to '{node.func.id}' is not allowed"
                )

        return True
