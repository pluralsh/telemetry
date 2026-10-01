//! Runs Tempo's TraceQL parser/validator examples against traces.
//!
//! Tempo is AGPL-3.0, so its test data is read from a local checkout instead of
//! being vendored. Point `TEMPO_SRC` at a checkout (defaults to a `tempo`
//! directory beside this workspace); the test is skipped when it is absent.

use std::{collections::BTreeMap, fs, path::PathBuf};

use plural_traces::traceql::{parse_syntax, validate};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    Valid,
    ParseFails,
    ValidateFails,
}

fn classify(query: &str) -> Outcome {
    match parse_syntax(query) {
        Err(_) => Outcome::ParseFails,
        Ok(parsed) => match validate(&parsed) {
            Ok(()) => Outcome::Valid,
            Err(_) => Outcome::ValidateFails,
        },
    }
}

fn examples() -> Option<BTreeMap<String, Vec<String>>> {
    let root = std::env::var_os("TEMPO_SRC").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../tempo"),
        PathBuf::from,
    );
    let path = root.join("pkg/traceql/test_examples.yaml");
    let Ok(source) = fs::read_to_string(&path) else {
        eprintln!("skipping: {} not found (set TEMPO_SRC)", path.display());
        return None;
    };
    let sections: BTreeMap<String, Option<Vec<String>>> =
        serde_yaml::from_str(&source).expect("test_examples.yaml");
    Some(
        sections
            .into_iter()
            .map(|(name, queries)| (name, queries.unwrap_or_default()))
            .collect(),
    )
}

/// Known gaps, matched by substring, with the reason. Any divergence not
/// covered here fails the test, as does a gap that no longer diverges.
const KNOWN_GAPS: &[(&str, &str)] = &[
    ("rate()", "TraceQL metrics are not implemented"),
    ("_over_time(", "TraceQL metrics are not implemented"),
    ("compare(", "TraceQL metrics are not implemented"),
    ("event.", "span event attributes are not queryable"),
    ("event:", "span event intrinsics are not queryable"),
    ("link.", "span link attributes are not queryable"),
    ("link:", "span link intrinsics are not queryable"),
    ("| { false }) ", "pipelines cannot be structural operands"),
];

/// A query both engines reject, but at a different stage (e.g. traces's parser
/// is more permissive and its validator catches the error).
fn rejected_at_other_stage(section: &str, outcome: Outcome) -> bool {
    matches!(
        (section, outcome),
        ("parse_fails", Outcome::ValidateFails) | ("validate_fails", Outcome::ParseFails)
    )
}

#[test]
fn tempo_test_examples() {
    let Some(sections) = examples() else {
        return;
    };
    let mut total = 0;
    let mut other_stage = 0;
    let mut superset = Vec::new();
    let mut known = BTreeMap::<&str, usize>::new();
    let mut unexpected = Vec::new();
    for (section, queries) in &sections {
        for query in queries {
            total += 1;
            let outcome = classify(query);
            let conforms = match section.as_str() {
                "valid" => outcome == Outcome::Valid,
                "parse_fails" => outcome == Outcome::ParseFails,
                "validate_fails" => outcome == Outcome::ValidateFails,
                "unsupported" => outcome != Outcome::Valid,
                _ => true,
            };
            if conforms {
                continue;
            }
            if let Some((pattern, _)) = KNOWN_GAPS
                .iter()
                .find(|(pattern, _)| query.contains(pattern))
            {
                *known.entry(pattern).or_default() += 1;
            } else if rejected_at_other_stage(section, outcome) {
                other_stage += 1;
            } else if section == "unsupported" {
                // Tempo parses these but refuses to run them; traces runs them.
                superset.push(query.as_str());
            } else {
                unexpected.push(format!("[{section}] {query:?} -> {outcome:?}"));
            }
        }
    }
    eprintln!(
        "tempo examples: {total} queries; {} known gaps {known:?}; {other_stage} rejected at \
         another stage; {} accepted beyond Tempo {superset:?}",
        known.values().sum::<usize>(),
        superset.len(),
    );
    let stale: Vec<_> = KNOWN_GAPS
        .iter()
        .filter(|(pattern, _)| !known.contains_key(pattern))
        .collect();
    assert!(
        unexpected.is_empty() && stale.is_empty(),
        "unexpected divergences:\n{}\nknown gaps that now conform: {stale:?}",
        unexpected.join("\n")
    );
}
