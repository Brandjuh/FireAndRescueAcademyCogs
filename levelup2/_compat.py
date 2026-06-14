"""Compatibility helpers that must run before third-party imports."""

from __future__ import annotations


def ensure_typing_extensions_sentinel() -> None:
    """Provide typing_extensions.Sentinel for environments with older typing_extensions.

    Some pydantic-core builds import ``typing_extensions.Sentinel`` at module
    import time. Older Red environments can have a pydantic-core version that
    expects this name while still carrying an older typing_extensions package.
    The import only needs the symbol to exist, so this lightweight fallback
    keeps the cog loadable until the environment dependency can be upgraded.
    """
    try:
        import typing_extensions
    except Exception:
        return

    if hasattr(typing_extensions, "Sentinel"):
        return

    class Sentinel:
        def __init__(self, name: str, repr: str | None = None):
            self.name = name
            self._repr = repr or name

        def __repr__(self) -> str:
            return self._repr

    typing_extensions.Sentinel = Sentinel
