#!/usr/bin/env python3
"""Loads data into a Vector cluster and checks every replica answers queries
the same way, before and after shard topology changes.

Every command takes the base URLs of individual replicas (writers and
readers alike); requests are spread across them so each one exercises the
cluster's forwarding to the replica that serves a namespace. Requests
authenticate as the tenants' user, and shard operations as the admin.
"""

import argparse
import base64
import json
import random
import sys
import time
import urllib.error
import urllib.request

NAMESPACES = [f"tenant-{i}" for i in range(8)]
TENANT = ("plrl", "e2e-tenant-secret")
OTHER = ("other", "e2e-other-secret")
ADMIN = ("admin", "e2e-admin-secret")
DIMENSIONS = 16
ROWS = 300
CATEGORIES = ["c0", "c1", "c2", "c3"]


class HTTPError(Exception):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status


def request(base, method, path, body=None, timeout=30, user=TENANT):
    data = None if body is None else json.dumps(body).encode()
    headers = {"content-type": "application/json"}
    if user is not None:
        token = base64.b64encode(":".join(user).encode()).decode()
        headers["authorization"] = f"Basic {token}"
    req = urllib.request.Request(f"{base}/api/v1{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        raise HTTPError(error.code, error.read().decode(errors="replace")) from None


def retry(what, fn, timeout=120):
    """Retries `fn` through transient failures: a shard moving between
    writers is briefly unavailable, and port forwards to new pods flap."""
    deadline = time.time() + timeout
    while True:
        try:
            return fn()
        except (HTTPError, OSError) as error:
            transient = not isinstance(error, HTTPError) or error.status in (502, 503, 504)
            if not transient or time.time() > deadline:
                raise RuntimeError(f"{what}: {error}") from None
            time.sleep(1)


def vector(rng):
    return [round(rng.uniform(-1, 1), 6) for _ in range(DIMENSIONS)]


def row(rng, ns, i, prefix="r"):
    return {
        "id": f"{prefix}{i}",
        "vector": vector(rng),
        "category": CATEGORIES[i % len(CATEGORIES)],
        "n": i,
        "ns": ns,
    }


def load(urls):
    for index, ns in enumerate(NAMESPACES):
        base = urls[index % len(urls)]
        retry(f"create {ns}", lambda: request(base, "PUT", f"/namespaces/{ns}", {
            "dimensions": DIMENSIONS,
            "distance_metric": "cosine_distance",
            "schema": {"category": {"type": "string"}, "ns": {"type": "string"}},
        }))
        rng = random.Random(ns)
        rows = [row(rng, ns, i) for i in range(ROWS)]
        for start in range(0, ROWS, 100):
            batch = rows[start:start + 100]
            writer = urls[(index + start // 100) % len(urls)]
            result = retry(f"write {ns}", lambda: request(writer, "POST", f"/namespaces/{ns}/write", {"upsert_rows": batch}))
            assert result["rows_upserted"] == len(batch), result
    base = urls[-1]
    retry("write private", lambda: request(base, "POST", "/namespaces/private/write", {
        "upsert_rows": [row(random.Random("private"), "private", 0)],
    }, user=OTHER))
    print(f"loaded {len(NAMESPACES)} namespaces x {ROWS} rows")


def normalize(rows):
    return [
        {key: round(value, 4) if key == "$dist" else value for key, value in sorted(row.items())}
        for row in rows
    ]


def suite(base):
    """The query results a replica gives, keyed by a stable description.
    Queries are eventually consistent, the only level a sharded cluster
    serves; row reads go to the owning writer."""
    results = {}
    listing = retry("list namespaces", lambda: request(base, "GET", "/namespaces"))
    results["namespaces"] = sorted(meta["name"] for meta in listing["namespaces"])
    for ns in NAMESPACES:
        rng = random.Random(f"query-{ns}")
        query = lambda body: retry(f"query {ns}", lambda: request(base, "POST", f"/namespaces/{ns}/query", body))
        results[f"{ns}/count"] = query({"aggregate_by": {"rows": {"op": "Count"}}})["aggregations"]["rows"]
        for i, category in enumerate(CATEGORIES[:3]):
            results[f"{ns}/knn/{i}"] = normalize(query({
                "rank_by": {"op": "kNN", "vector": vector(rng)},
                "filters": {"op": "Eq", "attribute": "category", "value": category},
                "limit": 10,
                "include_attributes": ["category", "n"],
            })["rows"])
        for i in range(2):
            results[f"{ns}/ann/{i}"] = normalize(query({
                "rank_by": {"op": "ANN", "vector": vector(rng)},
                "limit": 10,
                "include_attributes": ["n"],
            })["rows"])
        for row_id in ("r0", "r150", f"r{ROWS - 1}"):
            found = retry(f"get {ns}/{row_id}", lambda: request(base, "GET", f"/namespaces/{ns}/rows/{row_id}"))
            results[f"{ns}/row/{row_id}"] = {"id": found.get("id"), "n": found.get("n"), "vector": found.get("vector")}
    return results


def refuses_strong(base):
    try:
        request(base, "POST", f"/namespaces/{NAMESPACES[0]}/query", {
            "aggregate_by": {"rows": {"op": "Count"}}, "consistency": {"level": "strong"},
        })
    except HTTPError as error:
        return error.status == 400
    return False


def status(fn):
    try:
        fn()
    except HTTPError as error:
        return error.status
    except OSError:
        return 503
    return 200


def enforces_auth(base):
    """Problems with how `base` authenticates requests: other credentials,
    no credentials and undeclared namespaces are refused, and listings show
    what the caller may read."""
    ns = NAMESPACES[0]
    count = {"aggregate_by": {"rows": {"op": "Count"}}}
    query = lambda target, user: lambda: request(base, "POST", f"/namespaces/{target}/query", count, user=user)
    expectations = {
        "anonymous query": (status(query(ns, None)), 401),
        "query with a wrong password": (status(query(ns, ("plrl", "wrong"))), 401),
        "tenant querying private": (status(query("private", TENANT)), 401),
        "undeclared namespace": (status(query("undeclared", ADMIN)), 404),
        "tenant listing shards": (status(lambda: request(base, "GET", "/shards")), 401),
    }
    problems = [f"{what}: got {got}, expected {want}" for what, (got, want) in expectations.items() if got != want]
    listings = {
        "tenant": ({meta["name"] for meta in request(base, "GET", "/namespaces")["namespaces"]}, set(NAMESPACES)),
        "other": ({meta["name"] for meta in request(base, "GET", "/namespaces", user=OTHER)["namespaces"]}, {"private"}),
        "admin": ({meta["name"] for meta in request(base, "GET", "/namespaces", user=ADMIN)["namespaces"]}, set(NAMESPACES) | {"private"}),
    }
    problems += [f"{who} lists {sorted(got)}, expected {sorted(want)}" for who, (got, want) in listings.items() if got != want]
    return problems


def diff(expected, actual):
    keys = sorted(set(expected) | set(actual))
    return [key for key in keys if expected.get(key) != actual.get(key)]


def snapshot(urls, out):
    """Records the results once two consecutive runs agree, so the baseline
    is taken after readers catch up and background index maintenance
    settles."""
    previous = suite(urls[0])
    for _ in range(30):
        time.sleep(2)
        current = suite(urls[0])
        if current == previous:
            with open(out, "w") as file:
                json.dump(current, file, indent=1, sort_keys=True)
            print(f"baseline of {len(current)} results written to {out}")
            return
        print(f"results still changing: {diff(previous, current)[:5]}")
        previous = current
    sys.exit("results never settled")


def verify(urls, baseline, timeout):
    with open(baseline) as file:
        expected = json.load(file)
    for base in urls:
        if not refuses_strong(base):
            sys.exit(f"{base} served a strong query")
        deadline = time.time() + timeout
        while problems := retry(f"check auth at {base}", lambda: enforces_auth(base)):
            if time.time() > deadline:
                sys.exit(f"{base} authenticates wrongly: {problems}")
            time.sleep(2)
        deadline = time.time() + timeout
        while True:
            actual = suite(base)
            mismatched = diff(expected, actual)
            if not mismatched:
                print(f"results from {base} match the baseline ({len(actual)} results)")
                break
            if time.time() > deadline:
                for key in mismatched[:5]:
                    print(f"  {key}: expected {expected.get(key)} got {actual.get(key)}")
                sys.exit(f"results from {base} differ from the baseline in {len(mismatched)} results")
            # Readers trail their writers by up to a refresh interval.
            time.sleep(2)


def extend(urls, round_, timeout):
    """Writes new rows into every namespace after a topology change and reads
    them back through every replica: rows at once, queries once they catch
    up."""
    prefix = f"x{round_}-"
    for index, ns in enumerate(NAMESPACES):
        rng = random.Random(f"{ns}-{prefix}")
        rows = [row(rng, ns, i, prefix) for i in range(25)]
        base = urls[index % len(urls)]
        retry(f"write {ns}", lambda: request(base, "POST", f"/namespaces/{ns}/write", {"upsert_rows": rows}))
        for base in urls:
            found = retry(f"get {ns}", lambda: request(base, "GET", f"/namespaces/{ns}/rows/{rows[3]['id']}"))
            assert found["id"] == rows[3]["id"], found
            deadline = time.time() + timeout
            while True:
                hit = retry(f"query {ns}", lambda: request(base, "POST", f"/namespaces/{ns}/query", {
                    "rank_by": {"op": "kNN", "vector": rows[7]["vector"]}, "limit": 1,
                }))["rows"]
                if hit and hit[0]["id"] == rows[7]["id"]:
                    break
                assert time.time() < deadline, (ns, base, hit)
                time.sleep(1)
    print(f"round {round_}: new rows are readable from every replica")


def declared(urls, ns, timeout):
    """Waits until every replica lets OTHER write to the newly declared
    namespace `ns`, then deletes it again."""
    body = {"upsert_rows": [row(random.Random(ns), ns, 0)]}
    write = lambda base: lambda: request(base, "POST", f"/namespaces/{ns}/write", body, user=OTHER)
    for base in urls:
        deadline = time.time() + timeout
        while (got := status(write(base))) != 200:
            assert got in (404, 502, 503, 504) and time.time() < deadline, (base, got)
            time.sleep(2)
    retry(f"delete {ns}", lambda: request(urls[0], "DELETE", f"/namespaces/{ns}", user=OTHER))
    print(f"{ns} became writable on every replica without a restart")


def shards(urls):
    for meta in retry("list shards", lambda: request(urls[0], "GET", "/shards", user=ADMIN))["shards"]:
        print(json.dumps(meta, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["load", "snapshot", "verify", "extend", "declared", "shards"])
    parser.add_argument("--urls", required=True, help="comma-separated replica base URLs")
    parser.add_argument("--baseline")
    parser.add_argument("--namespace")
    parser.add_argument("--round", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    urls = args.urls.split(",")
    if args.command == "load":
        load(urls)
    elif args.command == "snapshot":
        snapshot(urls, args.baseline)
    elif args.command == "verify":
        verify(urls, args.baseline, args.timeout)
    elif args.command == "extend":
        extend(urls, args.round, args.timeout)
    elif args.command == "declared":
        declared(urls, args.namespace, args.timeout)
    else:
        shards(urls)


if __name__ == "__main__":
    main()
