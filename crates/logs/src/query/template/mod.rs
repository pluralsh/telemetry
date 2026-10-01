//! `line_format` and `label_format` templates.
//!
//! Go `text/template` is the default, Loki-compatible syntax: a Logos lexer
//! and LALRPOP grammar ported from Go's `text/template/parse` feed a resolver
//! that applies Go's parse-time checks and an evaluator that follows
//! `text/template/exec`. The `jinja` keyword selects Jinja, rendered by
//! minijinja. Both share Loki's template function library, and both compile
//! once when the query is parsed rather than once per row.

use std::collections::BTreeMap;
use std::rc::Rc;

use chrono::{TimeZone, Utc};

use super::{Row, lookup};
use crate::logql::TemplateSyntax;
use crate::{Error, Result};

mod functions;
mod go;
mod jinja;
mod printf;

/// Templates run once per row, so a render is bounded in work and output.
const MAX_STEPS: u64 = 100_000;
const MAX_OUTPUT_BYTES: usize = 1 << 20;

/// A compiled template; it renders against one row at a time.
pub(crate) struct Template(Engine);

enum Engine {
    Go(go::Template),
    Jinja(jinja::Template),
}

impl Template {
    pub(crate) fn compile(
        syntax: TemplateSyntax,
        source: &str,
    ) -> std::result::Result<Self, String> {
        Ok(Self(match syntax {
            TemplateSyntax::Go => Engine::Go(go::Template::compile(source)?),
            TemplateSyntax::Jinja => Engine::Jinja(jinja::Template::compile(source)?),
        }))
    }

    pub(super) fn render(&self, row: &Row) -> Result<String> {
        self.render_context(row)
    }

    fn render_context(&self, context: &dyn Context) -> Result<String> {
        match &self.0 {
            Engine::Go(template) => template.render(context),
            Engine::Jinja(template) => template.render(context),
        }
    }
}

/// The row a template renders: its line, timestamp and labels (stream,
/// parsed and structured metadata, as Loki's label builder merges them).
trait Context {
    fn line(&self) -> &str;
    fn timestamp_ns(&self) -> i64;
    fn get(&self, name: &str) -> Option<&str>;
    fn entries(&self) -> BTreeMap<String, String>;
}

impl Context for Row {
    fn line(&self) -> &str {
        &self.line
    }

    fn timestamp_ns(&self) -> i64 {
        self.timestamp_ns
    }

    fn get(&self, name: &str) -> Option<&str> {
        lookup(self, name)
    }

    fn entries(&self) -> BTreeMap<String, String> {
        let mut entries = self.metadata.clone();
        entries.extend(
            self.labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
        entries
    }
}

/// A template value. Integers and floats stay distinct because Go prints,
/// formats and compares them differently.
#[derive(Clone, Debug)]
enum Value {
    Nil,
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    /// Nanoseconds since the Unix epoch, printed as Go's `time.Time`.
    Time(i64),
    Map(Rc<BTreeMap<String, String>>),
    /// A decoded `fromJson` document.
    Json(Rc<serde_json::Value>),
}

impl Value {
    /// Go's `fmt.Sprint` of the value.
    fn text(&self) -> String {
        match self {
            Self::Nil => "<nil>".into(),
            Self::String(value) => value.clone(),
            Self::Int(value) => value.to_string(),
            Self::Float(value) => printf::format_float(*value),
            Self::Bool(value) => value.to_string(),
            Self::Time(value) => go_time_string(*value),
            Self::Map(map) => {
                let pairs = map.iter().map(|(key, value)| format!("{key}:{value}"));
                format!("map[{}]", pairs.collect::<Vec<_>>().join(" "))
            }
            Self::Json(value) => json_text(value),
        }
    }

    /// Go's type name, as `printf` reports it for a mismatched verb.
    fn type_name(&self) -> &'static str {
        match self {
            Self::Nil => "<nil>",
            Self::String(_) => "string",
            Self::Int(_) => "int",
            Self::Float(_) => "float64",
            Self::Bool(_) => "bool",
            Self::Time(_) => "time.Time",
            Self::Map(_) => "map[string]string",
            Self::Json(value) => match value.as_ref() {
                serde_json::Value::Null => "<nil>",
                serde_json::Value::Bool(_) => "bool",
                serde_json::Value::Number(_) => "float64",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Array(_) => "[]interface {}",
                serde_json::Value::Object(_) => "map[string]interface {}",
            },
        }
    }

    /// Go's `truth`: the zero value and empty collections are false.
    fn truthy(&self) -> bool {
        match self {
            Self::Nil => false,
            Self::String(value) => !value.is_empty(),
            Self::Int(value) => *value != 0,
            Self::Float(value) => *value != 0.0,
            Self::Bool(value) => *value,
            Self::Time(_) => true,
            Self::Map(map) => !map.is_empty(),
            Self::Json(value) => match value.as_ref() {
                serde_json::Value::Null => false,
                serde_json::Value::Bool(value) => *value,
                serde_json::Value::Number(value) => value.as_f64() != Some(0.0),
                serde_json::Value::String(value) => !value.is_empty(),
                serde_json::Value::Array(values) => !values.is_empty(),
                serde_json::Value::Object(values) => !values.is_empty(),
            },
        }
    }

    fn float(&self) -> Result<f64> {
        match self {
            Self::Int(value) => Ok(*value as f64),
            Self::Float(value) => Ok(*value),
            Self::String(value) => value
                .trim()
                .parse()
                .map_err(|_| Error::Query(format!("template value {value:?} is not numeric"))),
            Self::Json(value) => match value.as_ref() {
                serde_json::Value::Number(number) => Ok(number.as_f64().unwrap_or(0.0)),
                serde_json::Value::String(text) => Self::String(text.clone()).float(),
                _ => Err(self.not_numeric()),
            },
            _ => Err(self.not_numeric()),
        }
    }

    fn int(&self) -> Result<i64> {
        match self {
            Self::Int(value) => Ok(*value),
            Self::Float(value) => Ok(value.trunc() as i64),
            Self::String(value) => value
                .trim()
                .parse::<i64>()
                .or_else(|_| self.float().map(|value| value.trunc() as i64)),
            _ => self.float().map(|value| value.trunc() as i64),
        }
    }

    fn not_numeric(&self) -> Error {
        Error::Query(format!(
            "template value of type {} is not numeric",
            self.type_name()
        ))
    }

    /// A `fromJson` document node, unwrapped to a scalar where it is one.
    fn from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Nil,
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(value) => Self::Float(value.as_f64().unwrap_or(0.0)),
            serde_json::Value::String(value) => Self::String(value.clone()),
            _ => Self::Json(Rc::new(value.clone())),
        }
    }
}

/// Go's `time.Time.String()` in UTC.
fn go_time_string(timestamp_ns: i64) -> String {
    let time = Utc.timestamp_nanos(timestamp_ns);
    let fraction = format!("{:09}", time.timestamp_subsec_nanos());
    let fraction = fraction.trim_end_matches('0');
    let fraction = if fraction.is_empty() {
        String::new()
    } else {
        format!(".{fraction}")
    };
    format!("{}{fraction} +0000 UTC", time.format("%Y-%m-%d %H:%M:%S"))
}

/// Go's `fmt.Sprint` of a decoded `interface{}` document.
fn json_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "<nil>".into(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => printf::format_float(value.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(values) => {
            let items = values.iter().map(json_text).collect::<Vec<_>>();
            format!("[{}]", items.join(" "))
        }
        serde_json::Value::Object(values) => {
            let sorted = values.iter().collect::<BTreeMap<_, _>>();
            let pairs = sorted
                .into_iter()
                .map(|(key, value)| format!("{key}:{}", json_text(value)));
            format!("map[{}]", pairs.collect::<Vec<_>>().join(" "))
        }
    }
}

fn ensure_output_len(len: usize) -> Result<()> {
    if len > MAX_OUTPUT_BYTES {
        Err(Error::Query(format!(
            "template output exceeds {MAX_OUTPUT_BYTES} bytes"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) struct TestRow {
        pub line: String,
        pub timestamp_ns: i64,
        pub labels: BTreeMap<String, String>,
    }

    impl TestRow {
        pub fn new(labels: &[(&str, &str)]) -> Self {
            Self {
                line: "the line".into(),
                timestamp_ns: 1_500_000_000_123_000_000,
                labels: labels
                    .iter()
                    .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                    .collect(),
            }
        }
    }

    impl Context for TestRow {
        fn line(&self) -> &str {
            &self.line
        }

        fn timestamp_ns(&self) -> i64 {
            self.timestamp_ns
        }

        fn get(&self, name: &str) -> Option<&str> {
            self.labels.get(name).map(String::as_str)
        }

        fn entries(&self) -> BTreeMap<String, String> {
            self.labels.clone()
        }
    }

    pub(super) fn render(syntax: TemplateSyntax, source: &str, row: &TestRow) -> Result<String> {
        Template::compile(syntax, source)
            .map_err(Error::Query)?
            .render_context(row)
    }

    #[test]
    fn go_time_string_trims_fraction() {
        assert_eq!(
            go_time_string(1_500_000_000_123_000_000),
            "2017-07-14 02:40:00.123 +0000 UTC"
        );
        assert_eq!(go_time_string(0), "1970-01-01 00:00:00 +0000 UTC");
    }

    #[test]
    fn values_print_as_go_does() {
        assert_eq!(Value::Int(1_000_000).text(), "1000000");
        assert_eq!(Value::Float(1_000_000.0).text(), "1e+06");
        let json: serde_json::Value = serde_json::from_str(r#"{"b":[1,"x"],"a":null}"#).unwrap();
        assert_eq!(Value::Json(Rc::new(json)).text(), "map[a:<nil> b:[1 x]]");
    }
}
