//! The semantic checks Go's template parser makes, so a bad template fails
//! when the query is parsed rather than on every row: functions must exist,
//! variables must be in scope, `break`/`continue` must sit inside `range`,
//! declarations must fit their context and referenced templates must exist.

use std::collections::HashMap;

use super::super::functions::FUNCTIONS;
use super::TemplateError;
use super::ast::{Control, Item, Node, Operand, Pipeline};
use super::exec::BUILTINS;

pub(super) struct Tree {
    pub main: Vec<Node>,
    pub templates: HashMap<String, Vec<Node>>,
}

/// Functions bound to the current row rather than the library.
pub(super) const ROW_FUNCTIONS: &[&str] = &["__line__", "__timestamp__"];

pub(super) fn resolve(items: Vec<Item>) -> Result<Tree, TemplateError> {
    let mut main = Vec::new();
    let mut templates = HashMap::new();
    for item in items {
        match item {
            Item::Node(node) => main.push(node),
            Item::Define {
                name,
                body,
                position,
            } => define(&mut templates, name, body, position)?,
        }
    }
    hoist_blocks(&mut main, &mut templates)?;
    let mut checker = Checker {
        variables: Vec::new(),
        range_depth: 0,
        templates: &templates,
    };
    checker.template(&main)?;
    for body in templates.values() {
        checker.template(body)?;
    }
    Ok(Tree { main, templates })
}

fn define(
    templates: &mut HashMap<String, Vec<Node>>,
    name: String,
    body: Vec<Node>,
    position: usize,
) -> Result<(), TemplateError> {
    if templates.contains_key(&name) {
        return Err(TemplateError::new(
            position,
            format!("template: multiple definition of template {name:?}"),
        ));
    }
    templates.insert(name, body);
    Ok(())
}

/// Replaces each `{{block}}` with a call to the template it defines.
fn hoist_blocks(
    nodes: &mut [Node],
    templates: &mut HashMap<String, Vec<Node>>,
) -> Result<(), TemplateError> {
    for node in nodes {
        match node {
            Node::If(control) | Node::With(control) | Node::Range(control) => {
                hoist_blocks(&mut control.list, templates)?;
                hoist_blocks(&mut control.else_list, templates)?;
            }
            Node::Block { .. } => {
                let Node::Block {
                    name,
                    pipe,
                    mut body,
                    position,
                } = std::mem::replace(node, Node::Break(0))
                else {
                    unreachable!("matched block");
                };
                hoist_blocks(&mut body, templates)?;
                define(templates, name.clone(), body, position)?;
                *node = Node::Template {
                    name,
                    pipe: Some(pipe),
                    position,
                };
            }
            _ => {}
        }
    }
    Ok(())
}

struct Checker<'a> {
    variables: Vec<String>,
    range_depth: usize,
    templates: &'a HashMap<String, Vec<Node>>,
}

impl Checker<'_> {
    /// Each template body starts with only `$` in scope.
    fn template(&mut self, nodes: &[Node]) -> Result<(), TemplateError> {
        self.variables = vec!["$".into()];
        self.range_depth = 0;
        self.list(nodes)
    }

    fn list(&mut self, nodes: &[Node]) -> Result<(), TemplateError> {
        nodes.iter().try_for_each(|node| self.node(node))
    }

    fn node(&mut self, node: &Node) -> Result<(), TemplateError> {
        match node {
            Node::Text(_) => Ok(()),
            // An action's variables stay in scope until the enclosing `end`.
            Node::Action(pipe) => self.pipeline(pipe, 1, "command"),
            Node::If(control) | Node::With(control) => self.control(control, false),
            Node::Range(control) => self.control(control, true),
            Node::Template {
                name,
                pipe,
                position,
            } => {
                if !self.templates.contains_key(name) {
                    return Err(TemplateError::new(
                        *position,
                        format!("no such template {name:?}"),
                    ));
                }
                pipe.as_ref()
                    .map_or(Ok(()), |pipe| self.pipeline(pipe, 0, "template"))
            }
            Node::Block { .. } => unreachable!("blocks are hoisted"),
            Node::Break(position) | Node::Continue(position) if self.range_depth == 0 => {
                let keyword = if matches!(node, Node::Break(_)) {
                    "break"
                } else {
                    "continue"
                };
                Err(TemplateError::new(
                    *position,
                    format!("{{{{{keyword}}}}} outside {{{{range}}}}"),
                ))
            }
            Node::Break(_) | Node::Continue(_) => Ok(()),
        }
    }

    fn control(&mut self, control: &Control, range: bool) -> Result<(), TemplateError> {
        let mark = self.variables.len();
        let context = if range { "range" } else { "command" };
        self.pipeline(&control.pipe, if range { 2 } else { 1 }, context)?;
        if range {
            self.range_depth += 1;
        }
        self.list(&control.list)?;
        if range {
            self.range_depth -= 1;
        }
        self.list(&control.else_list)?;
        self.variables.truncate(mark);
        Ok(())
    }

    fn pipeline(
        &mut self,
        pipe: &Pipeline,
        max_decl: usize,
        context: &str,
    ) -> Result<(), TemplateError> {
        if pipe.decl.len() > max_decl {
            return Err(TemplateError::new(
                pipe.position,
                format!("too many declarations in {context}"),
            ));
        }
        for name in &pipe.decl {
            if pipe.assign {
                self.variable(name, pipe.position)?;
            } else {
                self.variables.push(name.clone());
            }
        }
        for (index, command) in pipe.commands.iter().enumerate() {
            if index > 0
                && matches!(
                    command.args[0],
                    Operand::Nil
                        | Operand::Bool(_)
                        | Operand::Int(_)
                        | Operand::Float(_)
                        | Operand::String(_)
                        | Operand::Dot
                )
            {
                return Err(TemplateError::new(
                    command.position,
                    format!("non executable command in pipeline stage {}", index + 1),
                ));
            }
            for operand in &command.args {
                self.operand(operand, command.position)?;
            }
        }
        Ok(())
    }

    fn operand(&mut self, operand: &Operand, position: usize) -> Result<(), TemplateError> {
        match operand {
            Operand::Function(name, position) => {
                let name = name.as_str();
                if FUNCTIONS.contains(&name)
                    || BUILTINS.contains(&name)
                    || ROW_FUNCTIONS.contains(&name)
                {
                    Ok(())
                } else {
                    Err(TemplateError::new(
                        *position,
                        format!("function {name:?} not defined"),
                    ))
                }
            }
            Operand::Variable(name, _) => self.variable(name, position),
            Operand::Pipe(pipe) => self.pipeline(pipe, 0, "parenthesized pipeline"),
            Operand::Chain(inner, _) => self.operand(inner, position),
            _ => Ok(()),
        }
    }

    fn variable(&self, name: &str, position: usize) -> Result<(), TemplateError> {
        if self.variables.iter().any(|variable| variable == name) {
            Ok(())
        } else {
            Err(TemplateError::new(
                position,
                format!("undefined variable {name:?}"),
            ))
        }
    }
}
