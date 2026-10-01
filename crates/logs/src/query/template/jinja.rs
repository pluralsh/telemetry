//! Jinja templates, rendered by minijinja.
//!
//! Each label is a top-level variable, alongside `__line__`, `__timestamp__`
//! and `__labels__` (every label, for iteration). Loki's function library is
//! available as functions (`trunc(3, x)`) and, where the name does not shadow
//! a Jinja builtin filter, as filters that append the piped value last, the
//! way a Go pipeline does (`x | trunc(3)`). Jinja's own filters keep their
//! Jinja meaning.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use minijinja::value::{Object, ObjectRepr, Rest};
use minijinja::{AutoEscape, Environment, ErrorKind, Value as JinjaValue};

use super::functions::{self, FUNCTIONS};
use super::printf::sprintf;
use super::{Context, MAX_STEPS, Value, ensure_output_len, go_time_string};
use crate::{Error, Result};

const NAME: &str = "template";

/// Library functions whose names Jinja already uses for a builtin filter.
const JINJA_FILTERS: &[&str] = &[
    "count",
    "default",
    "indent",
    "int",
    "lower",
    "max",
    "min",
    "replace",
    "round",
    "title",
    "trim",
    "upper",
    "urlencode",
];

pub(super) struct Template {
    environment: Environment<'static>,
    /// Top-level names the template reads; only these are bound per row.
    variables: Vec<String>,
}

impl Template {
    pub(super) fn compile(source: &str) -> std::result::Result<Self, String> {
        let mut environment = environment();
        environment
            .add_template_owned(NAME, source.to_owned())
            .map_err(|error| format!("template: {error}"))?;
        let variables = environment
            .get_template(NAME)
            .map_err(|error| format!("template: {error}"))?
            .undeclared_variables(false)
            .into_iter()
            .collect();
        Ok(Self {
            environment,
            variables,
        })
    }

    pub(super) fn render(&self, context: &dyn Context) -> Result<String> {
        let mut variables = BTreeMap::new();
        for name in &self.variables {
            let value = match name.as_str() {
                "__line__" => JinjaValue::from(context.line()),
                "__timestamp__" => JinjaValue::from_object(Timestamp(context.timestamp_ns())),
                "__labels__" => JinjaValue::from_serialize(context.entries()),
                label => match context.get(label) {
                    Some(value) => JinjaValue::from(value),
                    None => continue,
                },
            };
            variables.insert(name.as_str(), value);
        }
        let output = self
            .environment
            .get_template(NAME)
            .and_then(|template| template.render(variables))
            .map_err(|error| Error::Query(format!("template: {error}")))?;
        ensure_output_len(output.len())?;
        Ok(output)
    }
}

fn environment() -> Environment<'static> {
    let mut environment = Environment::new();
    environment.set_auto_escape_callback(|_| AutoEscape::None);
    environment.set_keep_trailing_newline(true);
    environment.set_fuel(Some(MAX_STEPS));
    for &name in FUNCTIONS {
        environment.add_function(name, move |arguments: Rest<JinjaValue>| {
            call(name, &arguments)
        });
        if !JINJA_FILTERS.contains(&name) {
            environment.add_filter(
                name,
                move |value: JinjaValue, arguments: Rest<JinjaValue>| {
                    let mut arguments = arguments.0;
                    arguments.push(value);
                    call(name, &arguments)
                },
            );
        }
    }
    environment.add_function("printf", |format: String, arguments: Rest<JinjaValue>| {
        let arguments = arguments.iter().map(from_jinja).collect::<Vec<_>>();
        sprintf(&format, &arguments).map_err(invalid_operation)
    });
    environment
}

fn call(name: &str, arguments: &[JinjaValue]) -> std::result::Result<JinjaValue, minijinja::Error> {
    let arguments = arguments.iter().map(from_jinja).collect::<Vec<_>>();
    functions::call(name, &arguments)
        .map(to_jinja)
        .map_err(invalid_operation)
}

fn invalid_operation(error: Error) -> minijinja::Error {
    let message = match error {
        Error::Query(message) => message,
        other => other.to_string(),
    };
    minijinja::Error::new(ErrorKind::InvalidOperation, message)
}

/// A row timestamp; it renders as Go's `time.Time` so the time functions
/// read the same in both syntaxes.
#[derive(Debug)]
struct Timestamp(i64);

impl Object for Timestamp {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Plain
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&go_time_string(self.0))
    }
}

fn from_jinja(value: &JinjaValue) -> Value {
    if let Some(timestamp) = value.downcast_object_ref::<Timestamp>() {
        return Value::Time(timestamp.0);
    }
    if value.is_undefined() {
        // An absent label reads as empty, as with Loki's `missingkey=zero`.
        return Value::String(String::new());
    }
    if value.is_none() {
        return Value::Nil;
    }
    if let Some(text) = value.as_str() {
        return Value::String(text.to_owned());
    }
    if value.is_integer()
        && let Some(integer) = value.as_i64()
    {
        return Value::Int(integer);
    }
    if value.is_number() {
        return Value::Float(f64::try_from(value.clone()).unwrap_or(f64::NAN));
    }
    if let minijinja::value::ValueKind::Bool = value.kind() {
        return Value::Bool(value.is_true());
    }
    match serde_json::to_value(value) {
        Ok(json @ (serde_json::Value::Array(_) | serde_json::Value::Object(_))) => {
            Value::Json(std::rc::Rc::new(json))
        }
        _ => Value::String(value.to_string()),
    }
}

fn to_jinja(value: Value) -> JinjaValue {
    match value {
        Value::Nil => JinjaValue::from(()),
        Value::String(value) => JinjaValue::from(value),
        Value::Int(value) => JinjaValue::from(value),
        Value::Float(value) => JinjaValue::from(value),
        Value::Bool(value) => JinjaValue::from(value),
        Value::Time(value) => JinjaValue::from_object(Timestamp(value)),
        Value::Map(map) => JinjaValue::from_serialize(map.as_ref()),
        Value::Json(json) => JinjaValue::from_serialize(json.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{TestRow, render};
    use crate::logql::TemplateSyntax;

    fn jinja(source: &str, labels: &[(&str, &str)]) -> String {
        render(TemplateSyntax::Jinja, source, &TestRow::new(labels))
            .unwrap_or_else(|error| panic!("{source}: {error}"))
    }

    #[test]
    fn labels_line_and_timestamp() {
        let labels = [("user", "alice"), ("level", "error")];
        assert_eq!(
            jinja("{{ user }}/{{ missing }}/{{ __line__ }}", &labels),
            "alice//the line"
        );
        assert_eq!(
            jinja("{{ __timestamp__ }}", &labels),
            "2017-07-14 02:40:00.123 +0000 UTC"
        );
        assert_eq!(
            jinja("{{ __timestamp__ | unixEpoch }}", &labels),
            "1500000000"
        );
        assert_eq!(
            jinja(
                "{% for k, v in __labels__ | items %}{{ k }}={{ v }};{% endfor %}",
                &labels
            ),
            "level=error;user=alice;"
        );
        assert_eq!(jinja("line\n", &labels), "line\n");
        assert_eq!(jinja("<{{ user }}>", &[("user", "<b>")]), "<<b>>");
    }

    #[test]
    fn jinja_control_flow_and_builtin_filters() {
        let labels = [("level", "error"), ("user", "alice")];
        assert_eq!(
            jinja(
                "{% if level == 'error' %}{{ user | upper }}{% else %}ok{% endif %}",
                &labels
            ),
            "ALICE"
        );
        assert_eq!(jinja("{{ missing | default('none') }}", &labels), "none");
        assert_eq!(
            jinja("{{ user | replace('a', 'A') | title }}", &labels),
            "Alice"
        );
    }

    #[test]
    fn loki_functions_and_filters() {
        let labels = [("user", "alice"), ("value", "10"), ("size", "2KiB")];
        assert_eq!(jinja("{{ user | trunc(3) }}", &labels), "ali");
        assert_eq!(jinja("{{ trunc(3, user) }}", &labels), "ali");
        assert_eq!(jinja("{{ value | add(2) }}", &labels), "12");
        assert_eq!(jinja("{{ size | bytes }}", &labels), "2048.0");
        assert_eq!(jinja("{{ divf(7, 2) }}", &labels), "3.5");
        assert_eq!(jinja("{{ user | b64enc }}", &labels), "YWxpY2U=");
        assert_eq!(
            jinja("{{ printf('%-6s|%03d', user, 7) }}", &labels),
            "alice |007"
        );
        assert_eq!(
            jinja(r#"{{ fromJson('{"a":{"b":2}}').a.b }}"#, &labels),
            "2"
        );
    }

    #[test]
    fn errors_and_limits() {
        let row = TestRow::new(&[]);
        assert!(render(TemplateSyntax::Jinja, "{% if %}", &row).is_err());
        assert!(render(TemplateSyntax::Jinja, "{{ div(1, 0) }}", &row).is_err());
        assert!(
            render(
                TemplateSyntax::Jinja,
                "{% for i in range(100000) %}{% for j in range(100) %}{% endfor %}{% endfor %}",
                &row
            )
            .unwrap_err()
            .to_string()
            .contains("fuel")
        );
    }
}
