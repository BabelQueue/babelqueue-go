#!/usr/bin/env python3
"""Validate a conformance results.json (ADR-0032).

usage: check-conformance-results.py <results.json> <vendor-dir> <expected-conformance-version>

Run from the repository root. Fails (exit 1) when results.json
  * does not conform to schema/results.schema.json,
  * reports a conformance_version other than the pinned one,
  * has a `fail` case,
  * omits a binding whose Go module is present in this repository (bindings are derived from
    the source tree, never hand-written), or lists one whose module is absent,
  * omits a section (or a case id) that the manifest puts in scope for the reported bindings.
"""
import json
import os
import sys

from jsonschema import Draft202012Validator

# Module directory -> binding it carries (schema enum). Any transport module added to the
# repository must be listed here, or the check fails on the unknown module.
# KEEP IN SYNC with `moduleBindings` / `nonTransportModules` in conformance_results_test.go:
# the test writes results.json from its own copy, this script re-derives the set
# independently as the release gate.
MODULE_BINDINGS = {
    "redis": "redis",
    "amqp": "amqp",
    "sqs": "sqs",
    "azureservicebus": "asb",
    "pulsar": "pulsar",
    "kafka": "kafka",
    "artemis": "artemis",
    "otel": "otel",
}

# Go modules that are not bindings: queue-library adapters and idempotency-store helpers.
NON_BINDING_MODULES = {"asynq", "machinery", "idempotency-redis", "idempotency-postgres"}


def case_ids(section, body):
    """The case-id rule of results.schema.json."""
    def named(prefix, fallback, items):
        return [prefix + (c["name"] if isinstance(c, dict) and isinstance(c.get("name"), str) else fallback(i))
                for i, c in enumerate(items)]

    if isinstance(body, list):
        return named("", lambda i: f"{section}#{i}", body)
    if not isinstance(body, dict):
        return []
    ids = []
    if isinstance(body.get("cases"), list):
        ids += named("", lambda i: f"{section}#{i}", body["cases"])
    for key, value in body.items():
        if key in ("description", "schema", "cases") or not isinstance(value, dict):
            continue
        if isinstance(value.get("cases"), list):
            ids += named(key + "/", str, value["cases"])
        else:
            ids.append(key)
    return ids


def derive_bindings():
    """Bindings carried by the source tree; fails on a module it cannot classify."""
    errors = []
    bindings = set()
    for entry in sorted(os.listdir(".")):
        if not os.path.isfile(os.path.join(entry, "go.mod")) or entry in NON_BINDING_MODULES:
            continue
        if entry in MODULE_BINDINGS:
            bindings.add(MODULE_BINDINGS[entry])
        else:
            errors.append(f"unknown Go module {entry}/: add it to MODULE_BINDINGS (a transport) "
                          "or NON_BINDING_MODULES in this script")
    return sorted(bindings), errors


def main():
    results_path, vendor, pinned = sys.argv[1:4]
    with open(results_path) as f:
        results = json.load(f)
    with open(os.path.join(vendor, "manifest.json")) as f:
        manifest = json.load(f)
    with open(os.path.join(vendor, "schema", "results.schema.json")) as f:
        schema = json.load(f)
    errors = [e.message for e in Draft202012Validator(schema).iter_errors(results)]

    if results.get("conformance_version") != pinned:
        errors.append(f"conformance_version {results.get('conformance_version')!r} != pinned {pinned!r}")

    derived, derive_errors = derive_bindings()
    errors += derive_errors
    if sorted(results.get("bindings", [])) != derived:
        errors.append(f"bindings {sorted(results.get('bindings', []))} != bindings derived from the source tree {derived}")

    carried = {"core", *derived}
    sections = results.get("sections", {})
    for name, meta in manifest["sections"].items():
        if meta["scope"] not in carried:
            if name in sections:
                errors.append(f"section {name!r} (scope {meta['scope']}) is out of scope and must be absent")
            continue
        if name not in sections:
            errors.append(f"in-scope section {name!r} is missing from results.json")
            continue
        body = {"cases": manifest["cases"]} if name == "cases" else manifest.get(name)
        missing = [i for i in case_ids(name, body) if i not in sections[name]]
        if missing:
            errors.append(f"section {name!r} is missing cases: {', '.join(missing)}")
    for name in sections:
        if name not in manifest["sections"]:
            errors.append(f"section {name!r} is not defined by the manifest")

    failed = [f"{s}/{i}" for s, cs in sections.items() for i, r in cs.items() if r.get("status") == "fail"]
    if failed:
        errors.append("failing cases: " + ", ".join(failed))

    if errors:
        print("results.json check FAILED:", file=sys.stderr)
        for e in errors:
            print(" - " + e, file=sys.stderr)
        return 1
    total = sum(len(cs) for cs in sections.values())
    print(f"results.json OK: {len(sections)} sections, {total} cases, bindings={results['bindings']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
