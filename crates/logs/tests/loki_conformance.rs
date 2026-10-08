//! Runs Loki's `logqltest` scripts (`pkg/logql/internal/logqltest/testdata`)
//! against line.
//!
//! Loki is AGPL-3.0, so its scripts are read from a local checkout instead of
//! being vendored. Point `LOKI_SRC` at a checkout (defaults to a `loki`
//! directory beside this workspace); the test is skipped when it is absent.
//!
//! Loki evaluates these with the `categorize-labels` encoding, which splits
//! stream, structured metadata, and parsed labels. Logs returns stream and
//! parsed labels together with metadata on each entry, so expected stream
//! results are compared with stream and parsed labels merged.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use plural_logs::{
    CompactionConfig, Config, Direction, Field, Fields, Label, Labels, LogBatch, LogDb, LogEntry,
    Namespace, PageConfig, QueryOptions, QueryRequest, QueryResult,
    logql::{Expr, parse_syntax},
};

const S: i64 = 1_000_000_000;
/// Script time zero; line rejects nothing at 0, but a realistic epoch keeps
/// segment arithmetic representative.
const EPOCH: i64 = 1_700_000_000 * S;

type LabelMap = BTreeMap<String, String>;

/// Known gaps, matched by substring of the query, with the reason. Any failure
/// not covered here fails the test, as does a gap that no longer fails.
const KNOWN_GAPS: &[(&str, &str)] = &[
    (
        "> bool on (app) group_",
        "Loki drops group_left/group_right included labels when the comparison uses bool; line \
         keeps them, matching Prometheus",
    ),
    (
        r#"count_over_time({app="a"} | json [1m])"#,
        "duplicate JSON keys: Loki keeps the first value, line keeps the last",
    ),
    (
        "approx_count_distinct(id,",
        "line counts distinct values exactly; Loki returns its HyperLogLog estimate",
    ),
];

#[derive(Debug)]
enum EvalKind {
    Instant(i64),
    Range {
        start: i64,
        end: i64,
        step: i64,
    },
    Select {
        start: i64,
        end: i64,
        direction: Direction,
    },
}

#[derive(Debug, Default)]
struct Expected {
    fail: bool,
    empty: bool,
    ordered: bool,
    scalar: Option<f64>,
    series: Vec<(LabelMap, Vec<Option<f64>>)>,
    lines: Vec<ExpectedLine>,
}

#[derive(Debug)]
struct ExpectedLine {
    labels: LabelMap,
    line: String,
    timestamp: i64,
    metadata: LabelMap,
}

#[derive(Debug)]
struct Eval {
    line: usize,
    kind: EvalKind,
    query: String,
    expected: Expected,
}

#[derive(Debug)]
struct LoadLine {
    labels: LabelMap,
    line: String,
    start: i64,
    step: i64,
    count: i64,
    metadata: LabelMap,
}

#[derive(Debug)]
enum Command {
    Clear,
    Load(Vec<LoadLine>),
    Eval(Box<Eval>),
}

fn loki_root() -> PathBuf {
    std::env::var_os("LOKI_SRC").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../loki"),
        PathBuf::from,
    )
}

fn scripts() -> Option<Vec<(String, String)>> {
    let dir = loki_root().join("pkg/logql/internal/logqltest/testdata");
    let Ok(entries) = fs::read_dir(&dir) else {
        eprintln!("skipping: {} not found (set LOKI_SRC)", dir.display());
        return None;
    };
    let mut scripts = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "logqltest"))
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            (name, fs::read_to_string(&path).unwrap())
        })
        .collect::<Vec<_>>();
    scripts.sort();
    Some(scripts)
}

/// Removes a `#` comment, ignoring `#` inside `"…"` or `` `…` ``.
fn strip_comment(line: &str) -> &str {
    let mut quote = None;
    let mut escaped = false;
    for (index, character) in line.char_indices() {
        match quote {
            Some('"') if escaped => escaped = false,
            Some('"') if character == '\\' => escaped = true,
            Some(open) if character == open => quote = None,
            Some(_) => {}
            None if character == '"' || character == '`' => quote = Some(character),
            None if character == '#' => return &line[..index],
            None => {}
        }
    }
    line
}

fn duration(source: &str) -> i64 {
    if source.trim_start_matches(['0', '.']).is_empty() {
        return 0;
    }
    common::time::parse_duration_ns(source).unwrap_or_else(|_| panic!("duration {source:?}"))
}

struct Cursor<'a> {
    rest: &'a str,
}

impl<'a> Cursor<'a> {
    fn skip_ws(&mut self) {
        self.rest = self.rest.trim_start();
    }

    fn eat(&mut self, prefix: &str) -> bool {
        self.skip_ws();
        if let Some(rest) = self.rest.strip_prefix(prefix) {
            self.rest = rest;
            true
        } else {
            false
        }
    }

    fn word(&mut self) -> &'a str {
        self.skip_ws();
        let end = self
            .rest
            .find(|c: char| c.is_whitespace() || c == ']')
            .unwrap_or(self.rest.len());
        let (word, rest) = self.rest.split_at(end);
        self.rest = rest;
        word
    }

    /// A `"…"` string with Go escapes decoded.
    fn escaped_string(&mut self) -> String {
        self.skip_ws();
        let mut chars = self.rest.char_indices();
        assert_eq!(chars.next().map(|(_, c)| c), Some('"'), "{:?}", self.rest);
        let mut output = String::new();
        let mut escaped = false;
        for (index, character) in chars {
            if escaped {
                output.push(match character {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    other => other,
                });
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                self.rest = &self.rest[index + 1..];
                return output;
            } else {
                output.push(character);
            }
        }
        panic!("unterminated string");
    }

    /// A log line: `"…"` or `` `…` ``, neither unescaped.
    fn raw_string(&mut self) -> String {
        self.skip_ws();
        let quote = self.rest.chars().next().expect("log line");
        assert!(quote == '"' || quote == '`', "{:?}", self.rest);
        let end = self.rest[1..].find(quote).expect("unterminated line") + 1;
        let line = self.rest[1..end].to_owned();
        self.rest = &self.rest[end + 1..];
        line
    }

    fn name(&mut self) -> String {
        self.skip_ws();
        if self.rest.starts_with('"') {
            return self.escaped_string();
        }
        let end = self
            .rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.' || c == '-'))
            .unwrap_or(self.rest.len());
        let (name, rest) = self.rest.split_at(end);
        self.rest = rest;
        name.to_owned()
    }

    /// `key="value"` pairs separated by commas or whitespace, up to `close`.
    fn pairs(&mut self, close: char) -> LabelMap {
        let mut pairs = LabelMap::new();
        loop {
            self.rest = self
                .rest
                .trim_start_matches(|c: char| c.is_whitespace() || c == ',');
            if let Some(rest) = self.rest.strip_prefix(close) {
                self.rest = rest;
                return pairs;
            }
            let name = self.name();
            assert!(self.eat("="), "expected = in {:?}", self.rest);
            let value = self.escaped_string();
            pairs.insert(name, value);
        }
    }

    fn labels(&mut self) -> LabelMap {
        assert!(self.eat("{"), "expected labels in {:?}", self.rest);
        self.pairs('}')
    }
}

fn parse_value(token: &str) -> Vec<Option<f64>> {
    let number = |text: &str| -> f64 {
        match text {
            "NaN" => f64::NAN,
            "Inf" | "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            _ => text.parse().unwrap_or_else(|_| panic!("value {text:?}")),
        }
    };
    if token == "_" {
        return vec![None];
    }
    let Some((base, count)) = token.split_once('x') else {
        return vec![Some(number(token))];
    };
    let count: usize = count.parse().unwrap();
    let split = base
        .char_indices()
        .skip(1)
        .find(|&(index, c)| {
            (c == '+' || c == '-') && !matches!(base.as_bytes()[index - 1], b'e' | b'E')
        })
        .map(|(index, _)| index);
    let (start, delta) = match split {
        Some(index) => (number(&base[..index]), number(&base[index..])),
        None => (number(base), 0.0),
    };
    (0..=count)
        .map(|index| Some(start + delta * index as f64))
        .collect()
}

fn parse_eval(header: &str, line: usize) -> Eval {
    let mut cursor = Cursor { rest: header };
    assert!(cursor.eat("eval"));
    let kind = match cursor.word() {
        "instant" => {
            assert!(cursor.eat("at"));
            EvalKind::Instant(duration(cursor.word()))
        }
        "range" => {
            assert!(cursor.eat("from"));
            let start = duration(cursor.word());
            assert!(cursor.eat("to"));
            let end = duration(cursor.word());
            assert!(cursor.eat("step"));
            EvalKind::Range {
                start,
                end,
                step: duration(cursor.word()),
            }
        }
        "select" => {
            assert!(cursor.eat("from"));
            let start = duration(cursor.word());
            assert!(cursor.eat("to"));
            let end = duration(cursor.word());
            let direction = match cursor.word() {
                "forward" => Direction::Forward,
                "backward" => Direction::Backward,
                other => panic!("direction {other:?}"),
            };
            EvalKind::Select {
                start,
                end,
                direction,
            }
        }
        other => panic!("eval kind {other:?}"),
    };
    Eval {
        line,
        kind,
        query: cursor.rest.trim().to_owned(),
        expected: Expected::default(),
    }
}

fn parse_expected(eval: &mut Eval, text: &str) {
    let expected = &mut eval.expected;
    if text.starts_with("expect fail") {
        expected.fail = true;
    } else if text == "expect empty" {
        expected.empty = true;
    } else if text == "expect ordered" {
        expected.ordered = true;
    } else if text.starts_with("expect values-toleration") || text.starts_with("skip ") {
        // Tolerances target Loki's sharded stack; line is compared exactly.
    } else if text.starts_with('{') {
        let mut cursor = Cursor { rest: text };
        let labels = cursor.labels();
        if matches!(eval.kind, EvalKind::Select { .. }) {
            let line = cursor.raw_string();
            assert!(cursor.eat("@"));
            let timestamp = duration(cursor.word());
            let mut metadata = LabelMap::new();
            let mut merged = labels;
            if cursor.eat("[metadata") {
                metadata = cursor.pairs(']');
            }
            if cursor.eat("[parsed") {
                merged.extend(cursor.pairs(']'));
            }
            expected.lines.push(ExpectedLine {
                labels: merged,
                line,
                timestamp,
                metadata,
            });
        } else {
            let values = cursor
                .rest
                .split_whitespace()
                .flat_map(parse_value)
                .collect();
            expected.series.push((labels, values));
        }
    } else {
        expected.scalar = parse_value(text)[0];
    }
}

fn parse_load(text: &str) -> LoadLine {
    let mut cursor = Cursor { rest: text };
    let labels = cursor.labels();
    let line = cursor.raw_string();
    assert!(cursor.eat("@"));
    let start = duration(cursor.word());
    let (mut step, mut count, mut metadata) = (0, 1, LabelMap::new());
    loop {
        if cursor.eat("[repeat") {
            assert!(cursor.eat("every"));
            step = duration(cursor.word());
            assert!(cursor.eat("for"));
            count = cursor.word().parse().unwrap();
            assert!(cursor.eat("]"));
        } else if cursor.eat("[metadata") {
            metadata = cursor.pairs(']');
        } else {
            break;
        }
    }
    LoadLine {
        labels,
        line,
        start,
        step,
        count,
        metadata,
    }
}

fn parse_script(source: &str) -> Vec<Command> {
    let mut commands = Vec::new();
    for (index, raw) in source.lines().enumerate() {
        let text = strip_comment(raw);
        if text.trim().is_empty() {
            continue;
        }
        let indented = text.starts_with([' ', '\t']);
        let text = text.trim();
        if !indented {
            match text {
                "clear" => commands.push(Command::Clear),
                "load" => commands.push(Command::Load(Vec::new())),
                _ if text.starts_with("eval ") => {
                    commands.push(Command::Eval(Box::new(parse_eval(text, index + 1))));
                }
                _ => panic!("line {}: unknown command {text:?}", index + 1),
            }
            continue;
        }
        match commands.last_mut() {
            Some(Command::Load(lines)) => lines.push(parse_load(text)),
            Some(Command::Eval(eval)) => parse_expected(eval, text),
            _ => panic!("line {}: indented line outside a block", index + 1),
        }
    }
    commands
}

static DATABASES: AtomicUsize = AtomicUsize::new(0);

async fn open_database() -> LogDb {
    let path = format!(
        "loki-conformance-{}",
        DATABASES.fetch_add(1, Ordering::Relaxed)
    );
    LogDb::open(Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path,
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        segment_duration: Duration::from_secs(3600),
        discovery_rollup: Some(Duration::from_secs(24 * 3600)),
        retention: None,
        write_buffer: Default::default(),
        block_cache_capacity_bytes: plural_logs::DEFAULT_BLOCK_CACHE_CAPACITY_BYTES,
        page: PageConfig::default(),
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
    })
    .await
    .unwrap()
}

fn to_labels(map: &LabelMap) -> Labels {
    Labels::new(
        map.iter()
            .map(|(name, value)| Label::new(name, value))
            .collect(),
    )
    .unwrap()
}

fn to_map<'a>(pairs: impl Iterator<Item = (&'a String, &'a String)>) -> LabelMap {
    pairs
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

async fn load(db: &LogDb, namespace: &Namespace, lines: &[LoadLine]) {
    let mut batches: BTreeMap<LabelMap, Vec<LogEntry>> = BTreeMap::new();
    for line in lines {
        let metadata = Fields::new(
            line.metadata
                .iter()
                .map(|(name, value)| Field::new(name, value))
                .collect(),
        )
        .unwrap();
        for index in 0..line.count {
            batches.entry(line.labels.clone()).or_default().push(
                LogEntry::with_structured_metadata(
                    EPOCH + line.start + index * line.step,
                    line.line.replace("{{.i}}", &index.to_string()),
                    metadata.clone(),
                ),
            );
        }
    }
    let batches = batches
        .into_iter()
        .map(|(labels, entries)| LogBatch::new(to_labels(&labels), entries))
        .collect();
    db.write(namespace, batches).await.unwrap();
}

fn approx_equal(expected: f64, actual: f64) -> bool {
    if expected.is_nan() || actual.is_nan() {
        return expected.is_nan() && actual.is_nan();
    }
    if expected == actual {
        return true;
    }
    let difference = (expected - actual).abs();
    difference <= 1e-9 || difference / expected.abs().max(actual.abs()) <= 1e-9
}

type Points = Vec<(i64, f64)>;

fn compare_series(
    expected: &BTreeMap<LabelMap, Points>,
    actual: &BTreeMap<LabelMap, Points>,
) -> Result<(), String> {
    if expected.keys().ne(actual.keys()) {
        return Err(format!(
            "series labels differ:\n      expected {:?}\n      actual   {:?}",
            expected.keys().collect::<Vec<_>>(),
            actual.keys().collect::<Vec<_>>()
        ));
    }
    for (labels, points) in expected {
        let found = &actual[labels];
        let matches = points.len() == found.len()
            && points
                .iter()
                .zip(found)
                .all(|(e, a)| e.0 == a.0 && approx_equal(e.1, a.1));
        if !matches {
            return Err(format!(
                "{labels:?}:\n      expected {points:?}\n      actual   {found:?}"
            ));
        }
    }
    Ok(())
}

fn check(eval: &Eval, result: Result<QueryResult, plural_logs::Error>) -> Result<(), String> {
    let expected = &eval.expected;
    let result = match result {
        Err(error) if expected.fail => {
            let _ = error;
            return Ok(());
        }
        Err(error) => return Err(format!("error: {error}")),
        Ok(_) if expected.fail => return Err("expected failure, query succeeded".to_owned()),
        Ok(result) => result,
    };
    if expected.empty {
        let empty = match &result {
            QueryResult::Streams(streams) => streams.is_empty(),
            QueryResult::Vector(vector) => vector.is_empty(),
            QueryResult::Matrix(matrix) => matrix.is_empty(),
            QueryResult::Scalar(_) => false,
        };
        return if empty {
            Ok(())
        } else {
            Err(format!("expected empty, got {result:?}"))
        };
    }
    if let Some(value) = expected.scalar {
        return match result {
            QueryResult::Scalar(sample) if approx_equal(value, sample.value) => Ok(()),
            other => Err(format!("expected scalar {value}, got {other:?}")),
        };
    }
    match (&eval.kind, result) {
        (EvalKind::Select { .. }, QueryResult::Streams(streams))
        | (EvalKind::Instant(_), QueryResult::Streams(streams)) => {
            type Entries = Vec<(i64, String, LabelMap)>;
            let mut wanted: BTreeMap<LabelMap, Entries> = BTreeMap::new();
            for line in &expected.lines {
                wanted.entry(line.labels.clone()).or_default().push((
                    line.timestamp,
                    line.line.clone(),
                    line.metadata.clone(),
                ));
            }
            let mut found: BTreeMap<LabelMap, Entries> = BTreeMap::new();
            for stream in streams {
                let labels = stream
                    .labels
                    .iter()
                    .map(|label| (label.name.clone(), label.value.clone()))
                    .collect::<LabelMap>();
                for entry in stream.entries {
                    let metadata = entry
                        .structured_metadata
                        .iter()
                        .map(|field| (field.name.clone(), field.value.clone()))
                        .collect();
                    found.entry(labels.clone()).or_default().push((
                        entry.timestamp_ns - EPOCH,
                        entry.line,
                        metadata,
                    ));
                }
            }
            if wanted == found {
                Ok(())
            } else {
                Err(format!(
                    "streams differ:\n      expected {wanted:?}\n      actual   {found:?}"
                ))
            }
        }
        (EvalKind::Instant(at), QueryResult::Vector(vector)) => {
            if expected.ordered {
                let wanted = expected
                    .series
                    .iter()
                    .map(|(labels, values)| (labels.clone(), values[0]))
                    .collect::<Vec<_>>();
                let found = vector
                    .iter()
                    .map(|sample| {
                        let labels = sample
                            .labels
                            .iter()
                            .map(|label| (label.name.clone(), label.value.clone()))
                            .collect::<LabelMap>();
                        (labels, Some(sample.sample.value))
                    })
                    .collect::<Vec<_>>();
                let matches = wanted.len() == found.len()
                    && wanted.iter().zip(&found).all(|(e, a)| {
                        e.0 == a.0 && approx_equal(e.1.unwrap_or(f64::NAN), a.1.unwrap())
                    });
                return if matches {
                    Ok(())
                } else {
                    Err(format!(
                        "ordered vector differs:\n      expected {wanted:?}\n      actual   {found:?}"
                    ))
                };
            }
            let wanted = expected
                .series
                .iter()
                .map(|(labels, values)| {
                    (
                        labels.clone(),
                        values.iter().flatten().map(|&value| (*at, value)).collect(),
                    )
                })
                .collect();
            let found = vector
                .into_iter()
                .map(|sample| {
                    (
                        to_map(
                            sample
                                .labels
                                .iter()
                                .map(|label| (&label.name, &label.value)),
                        ),
                        vec![(sample.sample.timestamp_ns - EPOCH, sample.sample.value)],
                    )
                })
                .collect();
            compare_series(&wanted, &found)
        }
        (EvalKind::Range { start, step, .. }, QueryResult::Matrix(matrix)) => {
            let wanted = expected
                .series
                .iter()
                .map(|(labels, values)| {
                    let points = values
                        .iter()
                        .enumerate()
                        .filter_map(|(index, value)| {
                            value.map(|value| (start + index as i64 * step, value))
                        })
                        .collect();
                    (labels.clone(), points)
                })
                .collect();
            let found = matrix
                .into_iter()
                .map(|series| {
                    (
                        to_map(
                            series
                                .labels
                                .iter()
                                .map(|label| (&label.name, &label.value)),
                        ),
                        series
                            .samples
                            .iter()
                            .map(|sample| (sample.timestamp_ns - EPOCH, sample.value))
                            .collect(),
                    )
                })
                .collect();
            compare_series(&wanted, &found)
        }
        (_, other) => Err(format!("unexpected result shape: {other:?}")),
    }
}

async fn run_eval(db: &LogDb, namespace: &Namespace, eval: &Eval) -> Result<(), String> {
    let mut options = QueryOptions {
        limit: 100_000,
        ..QueryOptions::default()
    };
    let request = match &eval.kind {
        EvalKind::Instant(at) => {
            let log = matches!(parse_syntax(&eval.query), Ok(query) if matches!(query.value, Expr::Log(_)));
            if log {
                QueryRequest::instant_logs(eval.query.clone(), EPOCH + at)
            } else {
                QueryRequest::instant(eval.query.clone(), EPOCH + at)
            }
        }
        EvalKind::Range { start, end, step } => {
            QueryRequest::range(eval.query.clone(), EPOCH + start, EPOCH + end, *step)
        }
        EvalKind::Select {
            start,
            end,
            direction,
        } => {
            options.direction = *direction;
            QueryRequest::range(eval.query.clone(), EPOCH + start, EPOCH + end, S)
        }
    };
    check(eval, db.query(namespace, &request, options).await)
}

#[tokio::test]
async fn loki_logqltest_scripts() {
    let Some(scripts) = scripts() else {
        return;
    };
    let namespace = Namespace::new("tenant").unwrap();
    let mut total = 0;
    let mut known = BTreeMap::<&str, usize>::new();
    let mut per_file = BTreeMap::<&str, (usize, usize)>::new();
    let mut unexpected = Vec::new();
    for (name, source) in &scripts {
        let mut db = open_database().await;
        for command in parse_script(source) {
            match command {
                Command::Clear => {
                    db.close().await.unwrap();
                    db = open_database().await;
                }
                Command::Load(lines) => load(&db, &namespace, &lines).await,
                Command::Eval(eval) => {
                    total += 1;
                    let counts = per_file.entry(name).or_default();
                    counts.0 += 1;
                    let Err(reason) = run_eval(&db, &namespace, &eval).await else {
                        continue;
                    };
                    counts.1 += 1;
                    if let Some((pattern, _)) = KNOWN_GAPS
                        .iter()
                        .find(|(pattern, _)| eval.query.contains(pattern))
                    {
                        *known.entry(pattern).or_default() += 1;
                    } else {
                        unexpected
                            .push(format!("{name}:{} {}\n    {reason}", eval.line, eval.query));
                    }
                }
            }
        }
        db.close().await.unwrap();
    }
    eprintln!(
        "loki logqltest: {total} evals; failures per file (evals, failed): {per_file:?}; known \
         gaps {known:?}"
    );
    let stale: Vec<_> = KNOWN_GAPS
        .iter()
        .filter(|(pattern, _)| !known.contains_key(pattern))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    assert!(
        unexpected.is_empty() && stale.is_empty(),
        "{} unexpected failures:\n{}\nknown gaps that now pass: {stale:?}",
        unexpected.len(),
        unexpected.join("\n")
    );
}
