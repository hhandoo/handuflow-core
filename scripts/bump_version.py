#!/usr/bin/env python3
"""Deprecated — use scripts/release.py instead."""

from __future__ import annotations

import sys


def main() -> int:
    print(
        "bump_version.py is deprecated.\n\n"
        "1. Edit RELEASE.toml with the next version and release notes\n"
        "2. Run: python scripts/release.py prepare\n",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
