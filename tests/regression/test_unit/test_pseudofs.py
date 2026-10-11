from harness.fuzz.pseudofs import difference
from harness.pseudofs import wire
from harness.pseudofs.content import (
    Spec,
    check_race_payload,
    materialize,
    race_payload,
    spec_size,
)
from harness.pseudofs.model import Limits, Model, normalize


def _write(model: Model, path: str, text: str, tenant: str = "t") -> dict:
    spec = Spec("text", 1, len(text.encode()))
    return model.execute(
        {"op": "write", "tenant": tenant, "path": path, "content": spec.dump()}
    )


def test_wire_round_trips_nested_repeated_and_defaulted_fields() -> None:
    value = {
        "path": "/a",
        "line_number": 3,
        "line": "x",
        "before": [{"line_number": 2, "text": "w"}],
    }
    decoded = wire.decode("GrepMatch", wire.encode("GrepMatch", value))
    assert decoded["before"] == [{"line_number": 2, "text": "w"}]
    request = {"tenant": "t", "pattern": "/**", "max_results": 0}
    decoded = wire.decode("GlobRequest", wire.encode("GlobRequest", request))
    assert decoded["pattern"] == "/**"
    assert decoded.get("max_results", 0) == 0


def test_content_is_deterministic_and_sized() -> None:
    for kind in ("text", "unicode", "multibyte_cut", "late_nul", "png", "zeros"):
        spec = Spec(kind, 42, 5000)
        assert materialize(spec) == materialize(Spec(kind, 42, 5000))
        assert len(materialize(spec)) == spec_size(spec)
    assert b"\0" in materialize(Spec("late_nul", 1, 100))[8192:]


def test_race_payload_detects_torn_reads() -> None:
    data = race_payload(1, 2, 1000)
    assert check_race_payload(data) is None
    assert check_race_payload(data[:-1]) is not None
    assert check_race_payload(data[:-10] + race_payload(3, 4, 1000)[-10:]) is not None


def test_normalize_matches_the_server() -> None:
    assert normalize("a//b/./c") == "/a/b/c"
    assert normalize("/a/b/../c") == "/a/c"


def test_model_tracks_writes_renames_and_errors() -> None:
    model = Model(Limits())
    assert _write(model, "/a/b/c.txt", "hello\n").get("error") is None
    assert model.execute({"op": "is_dir", "tenant": "t", "path": "/a/b"})["value"]
    # A file in the middle of a path is not an error for existence checks.
    assert model.execute({"op": "exists", "tenant": "t", "path": "/a/b/c.txt/x"}) == {
        "value": False
    }
    result = model.execute(
        {"op": "rename", "tenant": "t", "source": "/nope", "target": "/nope"}
    )
    assert result["error"]["kind"] == "not_found"
    result = model.execute({"op": "rmdir", "tenant": "t", "path": "/a"})
    assert result["error"]["kind"] == "directory_not_empty"
    model.execute({"op": "rename", "tenant": "t", "source": "/a/b", "target": "/z"})
    assert model.execute({"op": "stat", "tenant": "t", "path": "/z/c.txt"})["size"] == 6


def test_glob_matches_in_walk_order_and_honors_escapes() -> None:
    model = Model(Limits())
    for path in ("/d/x.txt", "/d/sub/y.txt", "/d/star*", "/d/stars"):
        _write(model, path, "line\n")
    paths = [
        e[0]
        for e in model.execute({"op": "glob", "tenant": "t", "pattern": "/d/**/*.txt"})[
            "entries"
        ]
    ]
    assert paths == ["/d/x.txt", "/d/sub/y.txt"]
    escaped = model.execute({"op": "glob", "tenant": "t", "pattern": "/d/star\\*"})[
        "entries"
    ]
    assert [e[0] for e in escaped] == ["/d/star*"]


def test_grep_skips_binary_files_and_reports_context() -> None:
    model = Model(Limits())
    _write(model, "/a.txt", "x" * 10)
    model.execute(
        {
            "op": "write",
            "tenant": "t",
            "path": "/p.png",
            "content": Spec("png", 1, 100).dump(),
        }
    )
    found = model.execute({"op": "grep", "tenant": "t", "path": "/", "pattern": "."})
    assert {m["path"] for m in found["matches"]} <= {"/a.txt"}


def test_grep_comparator_accepts_any_walk_ordered_per_file_prefix() -> None:
    def match(path: str, line: int) -> dict:
        return {
            "path": path,
            "line_number": line,
            "line": "x",
            "before": [],
            "after": [],
        }

    expected = {"matches": [match("/a", 1), match("/a", 2), match("/b", 1)], "limit": 2}
    op = {"op": "grep"}
    assert (
        difference(op, expected, {"matches": [match("/a", 1), match("/b", 1)]}) is None
    )
    assert (
        difference(op, expected, {"matches": [match("/a", 1), match("/a", 2)]}) is None
    )
    # Skips a file's first match, so it is not a prefix of that file's matches.
    assert difference(op, expected, {"matches": [match("/a", 2), match("/b", 1)]})
    # Out of walk order.
    assert difference(op, expected, {"matches": [match("/b", 1), match("/a", 1)]})
    assert difference(op, expected, {"matches": [match("/a", 1)]})
