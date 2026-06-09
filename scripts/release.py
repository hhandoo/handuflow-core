#!/usr/bin/env python3
"""Prepare a handuflow release from RELEASE.toml.

RELEASE.toml is the release control file: it holds the *next* version and notes.
After ``prepare``:

1. ``pyproject.toml`` is updated to that version
2. ``CHANGELOG.md`` gets a new dated section (used by GitHub Actions / PyPI)
3. ``RELEASE.toml`` is reset with a suggested patch bump for the following release

Examples::

    python scripts/release.py check      # validate RELEASE.toml only
    python scripts/release.py prepare    # apply version + changelog updates
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    tomllib = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
CHANGELOG = ROOT / "CHANGELOG.md"
RELEASE_CONTROL = ROOT / "RELEASE.toml"

VERSION_RE = re.compile(r"^version\s*=\s*\"([^\"]+)\"\s*$", re.MULTILINE)
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


@dataclass(frozen=True)
class PendingRelease:
    version: str
    notes: str


def read_current_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    match = VERSION_RE.search(text)
    if not match:
        raise SystemExit("Could not find [project] version in pyproject.toml")
    return match.group(1)


def parse_version(version: str) -> tuple[int, int, int]:
    if not SEMVER_RE.fullmatch(version):
        raise SystemExit(f"Invalid semver: {version!r} (expected MAJOR.MINOR.PATCH)")
    return tuple(int(part) for part in version.split("."))  # type: ignore[return-value]


def next_patch_version(version: str) -> str:
    major, minor, patch = parse_version(version)
    return f"{major}.{minor}.{patch + 1}"


def _load_toml(path: Path) -> dict:
    if tomllib is None:
        raise SystemExit(
            "RELEASE.toml requires Python 3.11+ (or run with the same Python as CI: 3.12)"
        )
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _notes_have_content(notes: str) -> bool:
    for line in notes.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped != "-":
            return True
    return False


def parse_release_control(data: dict) -> PendingRelease:
    version = data.get("version")
    if not isinstance(version, str) or not version.strip():
        raise SystemExit(f"{RELEASE_CONTROL.name} must set version = \"X.Y.Z\"")

    version = version.strip()
    parse_version(version)

    notes = data.get("notes")
    if not isinstance(notes, str):
        raise SystemExit(f'{RELEASE_CONTROL.name} must set notes = """...""" (markdown)')

    notes = notes.strip()
    if not notes or not _notes_have_content(notes):
        raise SystemExit(
            f"{RELEASE_CONTROL.name} notes are empty. "
            "Add at least one bullet under ### Added, ### Changed, or ### Fixed."
        )

    return PendingRelease(version=version, notes=notes)


def load_pending_release() -> PendingRelease:
    if not RELEASE_CONTROL.is_file():
        raise SystemExit(f"Missing {RELEASE_CONTROL.relative_to(ROOT)}")
    return parse_release_control(_load_toml(RELEASE_CONTROL))


def validate_pending_release(pending: PendingRelease, *, current: str | None = None) -> None:
    current_version = current if current is not None else read_current_version()
    if pending.version == current_version:
        raise SystemExit(
            f"Next version {pending.version} matches current pyproject.toml version. "
            f"Update version in {RELEASE_CONTROL.name}."
        )
    if parse_version(pending.version) <= parse_version(current_version):
        raise SystemExit(
            f"Next version {pending.version} must be greater than current {current_version}"
        )
    if f"## [{pending.version}]" in CHANGELOG.read_text(encoding="utf-8"):
        raise SystemExit(f"CHANGELOG.md already has a section for [{pending.version}]")


def write_pyproject_version(new_version: str) -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    match = VERSION_RE.search(text)
    if not match:
        raise SystemExit("Could not find [project] version in pyproject.toml")
    old_version = match.group(1)
    PYPROJECT.write_text(
        VERSION_RE.sub(f'version = "{new_version}"', text, count=1),
        encoding="utf-8",
    )
    return old_version


def insert_changelog_section(version: str, notes: str) -> None:
    today = date.today().isoformat()
    section = f"## [{version}] - {today}\n\n{notes.rstrip()}\n\n"
    changelog = CHANGELOG.read_text(encoding="utf-8")
    marker = "## [Unreleased]"
    index = changelog.find(marker)
    if index == -1:
        raise SystemExit("CHANGELOG.md is missing an [Unreleased] section")
    insert_at = index + len(marker)
    trailing = changelog[insert_at:]
    if trailing.startswith("\n\n"):
        insert_at += 2
    elif trailing.startswith("\n"):
        insert_at += 1
    updated = changelog[:insert_at] + section + changelog[insert_at:]
    CHANGELOG.write_text(updated, encoding="utf-8")


def reset_release_control(released_version: str) -> None:
    suggested = next_patch_version(released_version)
    RELEASE_CONTROL.write_text(
        f"""# HanduFlow release control file.
# Set the next version and release notes, then run: python scripts/release.py prepare

version = "{suggested}"

notes = \"\"\"
### Added
-

### Changed
-

### Fixed
-
\"\"\"
""",
        encoding="utf-8",
    )


def cmd_check(_: argparse.Namespace) -> int:
    current = read_current_version()
    pending = load_pending_release()
    validate_pending_release(pending, current=current)
    print(f"Current version: {current}")
    print(f"Next release:    {pending.version}")
    print(f"Release notes ({RELEASE_CONTROL.name}):")
    print()
    print(pending.notes)
    return 0


def cmd_prepare(args: argparse.Namespace) -> int:
    current = read_current_version()
    pending = load_pending_release()
    validate_pending_release(pending, current=current)

    if args.dry_run:
        print(f"[dry-run] Would bump {current} → {pending.version}")
        print(f"[dry-run] Would update {CHANGELOG.name} and reset {RELEASE_CONTROL.name}")
        print()
        print(pending.notes)
        return 0

    old_version = write_pyproject_version(pending.version)
    insert_changelog_section(pending.version, pending.notes)
    reset_release_control(pending.version)

    print(f"Released {old_version} → {pending.version}")
    print(f"  updated {PYPROJECT.relative_to(ROOT)}")
    print(f"  updated {CHANGELOG.relative_to(ROOT)}")
    print(f"  reset   {RELEASE_CONTROL.relative_to(ROOT)} (next suggested: {next_patch_version(pending.version)})")
    print()
    print("Next steps:")
    print("  git checkout dev")
    print("  git add pyproject.toml CHANGELOG.md RELEASE.toml")
    print(f'  git commit -m "Release {pending.version}"')
    print("  git push origin dev")
    print("  # Open PR dev → main; merge when CI passes to publish PyPI")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare handuflow releases from RELEASE.toml",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="Validate RELEASE.toml without changing files")
    check.set_defaults(func=cmd_check)

    prepare = sub.add_parser("prepare", help="Apply version bump and changelog update")
    prepare.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned changes without writing files",
    )
    prepare.set_defaults(func=cmd_prepare)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
