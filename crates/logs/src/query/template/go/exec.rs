//! Template execution after `text/template/exec`.

use std::collections::BTreeMap;
use std::rc::Rc;

use super::super::functions::{self, query_escape};
use super::super::printf::{sprint, sprintf, sprintln};
use super::super::{Context, MAX_STEPS, Value, ensure_output_len};
use super::ast::{Command, Control, Node, Operand, Pipeline};
use super::resolve::Tree;
use crate::{Error, Result};

/// Go's predefined template functions (`call` is omitted: templates here
/// never hold function values).
pub(super) const BUILTINS: &[&str] = &[
    "and", "eq", "ge", "gt", "html", "index", "js", "le", "len", "lt", "ne", "not", "or", "print",
    "printf", "println", "slice", "urlquery",
];

/// `{{template}}` recursion limit.
const MAX_DEPTH: usize = 100;

pub(super) fn execute(tree: &Tree, context: &dyn Context) -> Result<String> {
    let mut state = State {
        tree,
        context,
        root: None,
        variables: vec![("$".into(), Data::Root)],
        output: String::new(),
        steps: 0,
        depth: 0,
    };
    state.list(&tree.main, &Data::Root)?;
    Ok(state.output)
}

/// The dot: the row itself until it is needed as a value, or a value.
#[derive(Clone)]
enum Data {
    Root,
    Value(Value),
}

enum Flow {
    Next,
    Break,
    Continue,
}

struct State<'a> {
    tree: &'a Tree,
    context: &'a dyn Context,
    root: Option<Rc<BTreeMap<String, String>>>,
    variables: Vec<(String, Data)>,
    output: String,
    steps: u64,
    depth: usize,
}

fn error(message: impl Into<String>) -> Error {
    Error::Query(format!("template: {}", message.into()))
}

impl State<'_> {
    fn step(&mut self) -> Result<()> {
        self.steps += 1;
        if self.steps > MAX_STEPS {
            Err(error(format!(
                "execution budget of {MAX_STEPS} steps exceeded"
            )))
        } else {
            Ok(())
        }
    }

    fn list(&mut self, nodes: &[Node], dot: &Data) -> Result<Flow> {
        for node in nodes {
            match self.node(node, dot)? {
                Flow::Next => {}
                flow => return Ok(flow),
            }
        }
        Ok(Flow::Next)
    }

    fn node(&mut self, node: &Node, dot: &Data) -> Result<Flow> {
        self.step()?;
        match node {
            Node::Text(text) => self.write(text)?,
            Node::Action(pipe) => {
                let value = self.pipeline(pipe, dot)?;
                if pipe.decl.is_empty() {
                    let text = match value {
                        Data::Value(Value::Nil) => "<no value>".to_owned(),
                        value => self.value(value).text(),
                    };
                    self.write(&text)?;
                }
            }
            Node::If(control) => return self.if_or_with(control, dot, false),
            Node::With(control) => return self.if_or_with(control, dot, true),
            Node::Range(control) => return self.range(control, dot),
            Node::Template { name, pipe, .. } => {
                let dot = match pipe {
                    Some(pipe) => self.pipeline(pipe, dot)?,
                    None => Data::Value(Value::Nil),
                };
                if self.depth >= MAX_DEPTH {
                    return Err(error(format!(
                        "exceeded maximum template depth ({MAX_DEPTH})"
                    )));
                }
                let tree = self.tree;
                let body = &tree.templates[name];
                let variables =
                    std::mem::replace(&mut self.variables, vec![("$".into(), dot.clone())]);
                self.depth += 1;
                let result = self.list(body, &dot);
                self.depth -= 1;
                self.variables = variables;
                result?;
            }
            Node::Block { .. } => unreachable!("blocks are hoisted"),
            Node::Break(_) => return Ok(Flow::Break),
            Node::Continue(_) => return Ok(Flow::Continue),
        }
        Ok(Flow::Next)
    }

    fn write(&mut self, text: &str) -> Result<()> {
        ensure_output_len(self.output.len() + text.len())?;
        self.output.push_str(text);
        Ok(())
    }

    fn if_or_with(&mut self, control: &Control, dot: &Data, with: bool) -> Result<Flow> {
        let mark = self.variables.len();
        let value = self.pipeline(&control.pipe, dot)?;
        let flow = if self.truth(&value) {
            let dot = if with { value } else { dot.clone() };
            self.list(&control.list, &dot)
        } else {
            self.list(&control.else_list, dot)
        };
        self.variables.truncate(mark);
        flow
    }

    fn range(&mut self, control: &Control, dot: &Data) -> Result<Flow> {
        let mark = self.variables.len();
        let value = self.pipeline(&control.pipe, dot)?;
        let value = self.value(value);
        let items: Vec<(Value, Value)> = match &value {
            Value::Nil => Vec::new(),
            Value::Map(map) => map
                .iter()
                .map(|(key, value)| (Value::String(key.clone()), Value::String(value.clone())))
                .collect(),
            Value::Json(json) => match json.as_ref() {
                serde_json::Value::Array(values) => values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| (Value::Int(index as i64), Value::from_json(value)))
                    .collect(),
                serde_json::Value::Object(values) => values
                    .iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .map(|(key, value)| (Value::String(key.clone()), Value::from_json(value)))
                    .collect(),
                _ => return Err(error(format!("range can't iterate over {}", value.text()))),
            },
            Value::Int(count) => {
                if control.pipe.decl.len() > 1 {
                    return Err(error(format!(
                        "can't use {count} to iterate over more than one variable"
                    )));
                }
                (0..(*count).max(0))
                    .map(|index| (Value::Int(index), Value::Int(index)))
                    .collect()
            }
            _ => return Err(error(format!("range can't iterate over {}", value.text()))),
        };
        if items.is_empty() {
            let flow = self.list(&control.else_list, dot);
            self.variables.truncate(mark);
            return flow;
        }
        for (key, element) in items {
            self.step()?;
            let declared = control.pipe.decl.len();
            if declared >= 1 {
                self.variables[mark + declared - 1].1 = Data::Value(element.clone());
            }
            if declared == 2 {
                self.variables[mark].1 = Data::Value(key);
            }
            let iteration = self.variables.len();
            let flow = self.list(&control.list, &Data::Value(element));
            self.variables.truncate(iteration);
            if matches!(flow?, Flow::Break) {
                break;
            }
        }
        self.variables.truncate(mark);
        Ok(Flow::Next)
    }

    fn pipeline(&mut self, pipe: &Pipeline, dot: &Data) -> Result<Data> {
        let mut value = None;
        for command in &pipe.commands {
            value = Some(self.command(command, dot, value)?);
        }
        let value = value.expect("pipelines have a command");
        for name in &pipe.decl {
            if pipe.assign {
                let slot = self
                    .variables
                    .iter_mut()
                    .rev()
                    .find(|(variable, _)| variable == name)
                    .ok_or_else(|| error(format!("undefined variable: {name}")))?;
                slot.1 = value.clone();
            } else {
                self.variables.push((name.clone(), value.clone()));
            }
        }
        Ok(value)
    }

    fn command(&mut self, command: &Command, dot: &Data, piped: Option<Data>) -> Result<Data> {
        let first = &command.args[0];
        if let Operand::Function(name, _) = first {
            return self.call(name, &command.args[1..], dot, piped);
        }
        if command.args.len() > 1 || piped.is_some() {
            return Err(error(format!(
                "can't give argument to non-function {first}"
            )));
        }
        self.operand(first, dot)
    }

    fn operand(&mut self, operand: &Operand, dot: &Data) -> Result<Data> {
        Ok(match operand {
            Operand::Nil => Data::Value(Value::Nil),
            Operand::Bool(value) => Data::Value(Value::Bool(*value)),
            Operand::Int(value) => Data::Value(Value::Int(*value)),
            Operand::Float(value) => Data::Value(Value::Float(*value)),
            Operand::String(value) => Data::Value(Value::String(value.clone())),
            Operand::Dot => dot.clone(),
            Operand::Field(names) => self.fields(dot.clone(), names)?,
            Operand::Variable(name, names) => {
                let value = self
                    .variables
                    .iter()
                    .rev()
                    .find(|(variable, _)| variable == name)
                    .map(|(_, value)| value.clone())
                    .ok_or_else(|| error(format!("undefined variable: {name}")))?;
                self.fields(value, names)?
            }
            Operand::Function(name, _) => self.call(name, &[], dot, None)?,
            Operand::Pipe(pipe) => self.pipeline(pipe, dot)?,
            Operand::Chain(inner, names) => {
                let value = self.operand(inner, dot)?;
                self.fields(value, names)?
            }
        })
    }

    fn fields(&mut self, mut receiver: Data, names: &[String]) -> Result<Data> {
        for name in names {
            receiver = Data::Value(match receiver {
                // Loki's `missingkey=zero` over a string map: absent is empty.
                Data::Root => Value::String(self.context.get(name).unwrap_or("").to_owned()),
                Data::Value(Value::Map(map)) => {
                    Value::String(map.get(name).cloned().unwrap_or_default())
                }
                Data::Value(Value::Json(json)) => match json.as_ref() {
                    serde_json::Value::Object(values) => {
                        values.get(name).map_or(Value::Nil, Value::from_json)
                    }
                    _ => {
                        return Err(error(format!(
                            "can't evaluate field {name} in type {}",
                            Value::Json(json.clone()).type_name()
                        )));
                    }
                },
                Data::Value(Value::Nil) => {
                    return Err(error(format!(
                        "nil pointer evaluating interface {{}}.{name}"
                    )));
                }
                Data::Value(other) => {
                    return Err(error(format!(
                        "can't evaluate field {name} in type {}",
                        other.type_name()
                    )));
                }
            });
        }
        Ok(receiver)
    }

    fn call(
        &mut self,
        name: &str,
        args: &[Operand],
        dot: &Data,
        piped: Option<Data>,
    ) -> Result<Data> {
        self.step()?;
        let arity = args.len() + usize::from(piped.is_some());
        match name {
            "and" | "or" => {
                if arity == 0 {
                    return Err(error(format!(
                        "wrong number of args for {name}: want at least 1 got 0"
                    )));
                }
                let mut last = Data::Value(Value::Nil);
                for arg in args {
                    last = self.operand(arg, dot)?;
                    if self.truth(&last) == (name == "or") {
                        return Ok(last);
                    }
                }
                return Ok(piped.unwrap_or(last));
            }
            "__line__" | "__timestamp__" => {
                if arity != 0 {
                    return Err(error(format!(
                        "wrong number of args for {name}: want 0 got {arity}"
                    )));
                }
                return Ok(Data::Value(if name == "__line__" {
                    Value::String(self.context.line().to_owned())
                } else {
                    Value::Time(self.context.timestamp_ns())
                }));
            }
            _ => {}
        }
        let mut values = Vec::with_capacity(arity);
        for arg in args {
            let value = self.operand(arg, dot)?;
            values.push(self.value(value));
        }
        if let Some(piped) = piped {
            values.push(self.value(piped));
        }
        let result = match builtin(name, &values) {
            Some(result) => result,
            None => functions::call(name, &values),
        };
        result.map(Data::Value).map_err(|failure| match failure {
            Error::Query(message) if !message.starts_with("template:") => {
                error(format!("error calling {name}: {message}"))
            }
            other => other,
        })
    }

    /// Materializes the row's label map the first time it is used as a value.
    fn value(&mut self, data: Data) -> Value {
        match data {
            Data::Value(value) => value,
            Data::Root => Value::Map(
                self.root
                    .get_or_insert_with(|| Rc::new(self.context.entries()))
                    .clone(),
            ),
        }
    }

    fn truth(&mut self, data: &Data) -> bool {
        self.value(data.clone()).truthy()
    }
}

fn builtin(name: &str, args: &[Value]) -> Option<Result<Value>> {
    BUILTINS.contains(&name).then(|| run_builtin(name, args))
}

fn run_builtin(name: &str, args: &[Value]) -> Result<Value> {
    let arity = |count: usize| {
        if args.len() == count {
            Ok(())
        } else {
            Err(Error::Query(format!(
                "wrong number of args for {name}: want {count} got {}",
                args.len()
            )))
        }
    };
    match name {
        "not" => {
            arity(1)?;
            Ok(Value::Bool(!args[0].truthy()))
        }
        "len" => {
            arity(1)?;
            length(&args[0]).map(|len| Value::Int(len as i64))
        }
        "index" => {
            let (first, keys) = args.split_first().ok_or_else(|| {
                Error::Query("wrong number of args for index: want at least 1 got 0".into())
            })?;
            keys.iter().try_fold(first.clone(), index)
        }
        "slice" => slice(args),
        "print" => Ok(Value::String(sprint(args))),
        "println" => Ok(Value::String(sprintln(args))),
        "printf" => {
            let (format, rest) = args.split_first().ok_or_else(|| {
                Error::Query("wrong number of args for printf: want at least 1 got 0".into())
            })?;
            sprintf(&format.text(), rest).map(Value::String)
        }
        "html" => Ok(Value::String(html_escape(&eval_args(args)))),
        "js" => Ok(Value::String(js_escape(&eval_args(args)))),
        "urlquery" => Ok(Value::String(query_escape(&eval_args(args)))),
        "eq" => {
            let (first, rest) = args
                .split_first()
                .filter(|(_, rest)| !rest.is_empty())
                .ok_or_else(|| Error::Query("missing argument for comparison".into()))?;
            for other in rest {
                if equal(first, other)? {
                    return Ok(Value::Bool(true));
                }
            }
            Ok(Value::Bool(false))
        }
        "ne" => {
            arity(2)?;
            equal(&args[0], &args[1]).map(|equal| Value::Bool(!equal))
        }
        "lt" | "le" | "gt" | "ge" => {
            arity(2)?;
            let ordering = compare(&args[0], &args[1])?;
            Ok(Value::Bool(match name {
                "lt" => ordering.is_lt(),
                "le" => ordering.is_le(),
                "gt" => ordering.is_gt(),
                _ => ordering.is_ge(),
            }))
        }
        _ => unreachable!("`and` and `or` short-circuit before their arguments are evaluated"),
    }
}

/// Go's `evalArgs`: a single string passes through, anything else is `Sprint`ed.
fn eval_args(args: &[Value]) -> String {
    match args {
        [Value::String(value)] => value.clone(),
        _ => sprint(args),
    }
}

fn length(value: &Value) -> Result<usize> {
    Ok(match value {
        Value::String(value) => value.len(),
        Value::Map(map) => map.len(),
        Value::Json(json) => match json.as_ref() {
            serde_json::Value::Array(values) => values.len(),
            serde_json::Value::Object(values) => values.len(),
            serde_json::Value::String(value) => value.len(),
            _ => return Err(Error::Query(format!("len of type {}", value.type_name()))),
        },
        other => return Err(Error::Query(format!("len of type {}", other.type_name()))),
    })
}

fn index(item: Value, key: &Value) -> Result<Value> {
    let position = |len: usize| -> Result<usize> {
        match key {
            Value::Int(index) if *index >= 0 && (*index as usize) < len => Ok(*index as usize),
            Value::Int(index) => Err(Error::Query(format!("index out of range: {index}"))),
            other => Err(Error::Query(format!(
                "cannot index slice/array with type {}",
                other.type_name()
            ))),
        }
    };
    let map_key = || match key {
        Value::String(key) => Ok(key.clone()),
        other => Err(Error::Query(format!(
            "value has type {}; should be string",
            other.type_name()
        ))),
    };
    Ok(match &item {
        Value::Map(map) => Value::String(map.get(&map_key()?).cloned().unwrap_or_default()),
        Value::String(text) => Value::Int(i64::from(text.as_bytes()[position(text.len())?])),
        Value::Json(json) => match json.as_ref() {
            serde_json::Value::Array(values) => Value::from_json(&values[position(values.len())?]),
            serde_json::Value::Object(values) => {
                values.get(&map_key()?).map_or(Value::Nil, Value::from_json)
            }
            _ => {
                return Err(Error::Query(format!(
                    "can't index item of type {}",
                    item.type_name()
                )));
            }
        },
        Value::Nil => return Err(Error::Query("index of untyped nil".into())),
        other => {
            return Err(Error::Query(format!(
                "can't index item of type {}",
                other.type_name()
            )));
        }
    })
}

fn slice(args: &[Value]) -> Result<Value> {
    let (item, bounds) = args.split_first().ok_or_else(|| {
        Error::Query("wrong number of args for slice: want at least 1 got 0".into())
    })?;
    if bounds.len() > 2 {
        return Err(Error::Query("too many slice indexes".into()));
    }
    let len = match item {
        Value::String(text) => text.len(),
        Value::Json(json) => match json.as_ref() {
            serde_json::Value::Array(values) => values.len(),
            _ => {
                return Err(Error::Query(format!(
                    "can't slice item of type {}",
                    item.type_name()
                )));
            }
        },
        other => {
            return Err(Error::Query(format!(
                "can't slice item of type {}",
                other.type_name()
            )));
        }
    };
    let mut indexes = [0, len];
    for (slot, bound) in indexes.iter_mut().zip(bounds) {
        *slot = match bound {
            Value::Int(index) if *index >= 0 && (*index as usize) <= len => *index as usize,
            Value::Int(index) => return Err(Error::Query(format!("index out of range: {index}"))),
            other => {
                return Err(Error::Query(format!(
                    "cannot index slice/array with type {}",
                    other.type_name()
                )));
            }
        };
    }
    let [start, end] = indexes;
    if start > end {
        return Err(Error::Query(format!(
            "invalid slice index: {start} > {end}"
        )));
    }
    Ok(match item {
        Value::String(text) => Value::String(
            text.get(start..end)
                .ok_or_else(|| Error::Query("slice splits a UTF-8 character".into()))?
                .to_owned(),
        ),
        Value::Json(json) => {
            let serde_json::Value::Array(values) = json.as_ref() else {
                unreachable!("checked array");
            };
            Value::Json(Rc::new(serde_json::Value::Array(
                values[start..end].to_vec(),
            )))
        }
        _ => unreachable!("checked sliceable"),
    })
}

/// A comparable scalar, as Go's `basicKind` classifies it.
enum Basic<'a> {
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(&'a str),
    Time(i64),
}

fn basic(value: &Value) -> Result<Basic<'_>> {
    Ok(match value {
        Value::Nil => Basic::Nil,
        Value::Bool(value) => Basic::Bool(*value),
        Value::Int(value) => Basic::Int(*value),
        Value::Float(value) => Basic::Float(*value),
        Value::String(value) => Basic::String(value),
        Value::Time(value) => Basic::Time(*value),
        other => {
            return Err(Error::Query(format!(
                "non-comparable type {}",
                other.type_name()
            )));
        }
    })
}

fn incompatible() -> Error {
    Error::Query("incompatible types for comparison".into())
}

fn equal(left: &Value, right: &Value) -> Result<bool> {
    Ok(match (basic(left)?, basic(right)?) {
        (Basic::Nil, Basic::Nil) => true,
        (Basic::Nil, _) | (_, Basic::Nil) => false,
        (Basic::Bool(left), Basic::Bool(right)) => left == right,
        (Basic::Int(left), Basic::Int(right)) => left == right,
        (Basic::Float(left), Basic::Float(right)) => left == right,
        (Basic::String(left), Basic::String(right)) => left == right,
        (Basic::Time(left), Basic::Time(right)) => left == right,
        _ => return Err(incompatible()),
    })
}

fn compare(left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
    match (basic(left)?, basic(right)?) {
        (Basic::Int(left), Basic::Int(right)) => Ok(left.cmp(&right)),
        (Basic::Float(left), Basic::Float(right)) => left
            .partial_cmp(&right)
            .ok_or_else(|| Error::Query("invalid comparison of NaN".into())),
        (Basic::String(left), Basic::String(right)) => Ok(left.cmp(right)),
        (left, right) if std::mem::discriminant(&left) == std::mem::discriminant(&right) => {
            Err(Error::Query("invalid type for comparison".into()))
        }
        _ => Err(incompatible()),
    }
}

/// Go's `template.HTMLEscapeString`.
fn html_escape(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\0' => output.push('\u{FFFD}'),
            '"' => output.push_str("&#34;"),
            '\'' => output.push_str("&#39;"),
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            character => output.push(character),
        }
    }
    output
}

/// Go's `template.JSEscapeString`.
fn js_escape(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\\' => output.push_str(r"\\"),
            '\'' => output.push_str(r"\'"),
            '"' => output.push_str(r#"\""#),
            '<' => output.push_str(r"\u003C"),
            '>' => output.push_str(r"\u003E"),
            '&' => output.push_str(r"\u0026"),
            '=' => output.push_str(r"\u003D"),
            '\n' => output.push_str(r"\n"),
            '\r' => output.push_str(r"\r"),
            '\t' => output.push_str(r"\t"),
            character if u32::from(character) < 0x20 || character.is_control() => {
                output.push_str(&format!(r"\u{:04X}", u32::from(character)));
            }
            character => output.push(character),
        }
    }
    output
}
