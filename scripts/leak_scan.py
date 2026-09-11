#!/usr/bin/env python3
"""
scripts/leak_scan.py
===========================================================================
Fails the build if something that looks like a real credential or a real
internal identifier is committed.

This repository describes a system that belongs to an employer. The demo
data, schema names and business codes in it are illustrative on purpose, and
the failure mode worth guarding against is not malice — it is pasting a real
query into a file during a late edit and pushing it.

The rules below match on *shape*, never on specific values. A deny-list
containing the actual internal names would be a file that leaks exactly what
it was written to prevent, and it would go stale the moment a different
system was involved. Shapes generalise and stay safe to publish.

    python scripts/leak_scan.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

SKIP_DIRS = {
    ".git", ".local-run", "__pycache__", ".pytest_cache", ".ruff_cache",
    "target", "dbt_packages", "logs", "node_modules", ".venv", "venv",
}

SKIP_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".zip", ".jar", ".parquet"}

# Values that are meant to be here. A finding is suppressed when it contains
# one of these, so the allow-list holds the sensitive substring rather than
# every line it appears on.
#
# The demo password is a throwaway credential for a container that binds to
# localhost and holds nothing but generated rows. It is written in plain text
# on purpose: a demo that asks you to configure a secret before it will run
# is a demo nobody runs.
ALLOWED = {
    "LocalDev!Passw0rd1",
    "data-eng-alerts@example.com",
}

RULES: list[tuple[str, str, str]] = [
    (
        "corporate email address",
        r"[A-Za-z0-9._%+-]+@(?!example\.(?:com|org|net)\b)[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        "Use an example.com address. A real mailbox in a public repo is both a "
        "leak and a spam target.",
    ),
    (
        "Azure SQL / cloud database hostname",
        r"[A-Za-z0-9-]+\.(?:database\.windows\.net|rds\.amazonaws\.com|"
        r"azuredatabricks\.net|cloud\.databricks\.com)",
        "Real hostnames identify the employer's infrastructure. Keep them in "
        "secrets, not in source.",
    ),
    (
        "credential assigned inline",
        r"(?i)(?:password|passwd|pwd|secret|api[_-]?key|access[_-]?token)"
        r"\s*[:=]\s*['\"][^'\"$\{\s]{6,}['\"]",
        "Read it from a secret scope or an environment variable.",
    ),
    (
        "JDBC/ODBC connection string with an embedded password",
        r"(?i)(?:jdbc|odbc)[^\s'\"]*[;?&](?:password|pwd)=[^;'\"\s]+",
        "Build the URL from secrets at run time.",
    ),
    (
        "private key block",
        r"-----BEGIN (?:RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----",
        "Never commit private keys. Rotate this one — it is compromised.",
    ),
    (
        "Databricks personal access token",
        r"\bdapi[0-9a-f]{32}\b",
        "Rotate the token and read it from the environment.",
    ),
    (
        "AWS access key id",
        r"\bAKIA[0-9A-Z]{16}\b",
        "Rotate the key immediately and use an instance role.",
    ),
    (
        "personal workspace path",
        r"/Repos/[A-Za-z]+\.[A-Za-z]+/",
        "A firstname.lastname workspace path names an individual. Use a team "
        "path such as /Repos/data-eng/.",
    ),
    (
        "hardcoded private IP address",
        r"\b(?:10\.\d{1,3}|192\.168|172\.(?:1[6-9]|2\d|3[01]))\.\d{1,3}\.\d{1,3}\b",
        "Internal addressing belongs in configuration, not source.",
    ),
]

COMPILED = [(name, re.compile(pattern), hint) for name, pattern, hint in RULES]

# A hostname whose leading label is nothing but repeated zeros, repeated x's
# or the word "example" is a documentation placeholder. Recognising the shape
# is better than listing each placeholder in ALLOWED: an exception people have
# to add by hand is an exception people learn to add without reading.
PLACEHOLDER_HOST = re.compile(
    r"\b(?:[a-z]+-)?(?:0+|x+|example)(?:\.\d+)?\.[a-z0-9.-]*"
    r"(?:database\.windows\.net|rds\.amazonaws\.com|azuredatabricks\.net|cloud\.databricks\.com)",
    re.IGNORECASE,
)


def iter_files():
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.suffix.lower() in SKIP_SUFFIXES:
            continue
        if path == Path(__file__).resolve():
            continue
        yield path


def scan() -> list[str]:
    findings = []

    for path in iter_files():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue

        for lineno, line in enumerate(text.splitlines(), start=1):
            for name, pattern, hint in COMPILED:
                for match in pattern.finditer(line):
                    if any(allowed in match.group(0) for allowed in ALLOWED):
                        continue
                    if PLACEHOLDER_HOST.fullmatch(match.group(0)):
                        continue
                    rel = path.relative_to(REPO_ROOT)
                    findings.append(
                        f"{rel}:{lineno}  {name}\n"
                        f"    matched: {match.group(0)[:70]}\n"
                        f"    {hint}"
                    )

    return findings


def main() -> int:
    findings = scan()

    if not findings:
        print(f"leak scan: clean ({len(COMPILED)} rules)")
        return 0

    print(f"leak scan: {len(findings)} finding(s)\n")
    for finding in findings:
        print(finding + "\n")
    print(
        "If a match is intentional, add the exact string to ALLOWED in this "
        "file with a comment explaining why."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
