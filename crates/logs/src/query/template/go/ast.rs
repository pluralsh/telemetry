//! Go `text/template/parse` nodes. Positions are byte offsets in the source.

pub(super) enum Item {
    Node(Node),
    Define {
        name: String,
        body: Vec<Node>,
        position: usize,
    },
}

pub(super) enum Node {
    Text(String),
    /// Prints its pipeline unless the pipeline declares variables.
    Action(Pipeline),
    If(Control),
    With(Control),
    Range(Control),
    Template {
        name: String,
        pipe: Option<Pipeline>,
        position: usize,
    },
    /// `{{block}}` defines a template and executes it in place; the resolver
    /// lowers it to a definition plus a [`Node::Template`].
    Block {
        name: String,
        pipe: Pipeline,
        body: Vec<Node>,
        position: usize,
    },
    Break(usize),
    Continue(usize),
}

/// `if`, `with` and `range`; `else if` and `else with` nest in `else_list`.
pub(super) struct Control {
    pub pipe: Pipeline,
    pub list: Vec<Node>,
    pub else_list: Vec<Node>,
}

pub(super) struct Pipeline {
    pub position: usize,
    /// Declared (`:=`) or assigned (`=`) variables, including the `$`.
    pub decl: Vec<String>,
    pub assign: bool,
    pub commands: Vec<Command>,
}

pub(super) struct Command {
    pub position: usize,
    pub args: Vec<Operand>,
}

pub(super) enum Operand {
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Dot,
    /// `.a.b`
    Field(Vec<String>),
    /// `$x.a.b`
    Variable(String, Vec<String>),
    Function(String, usize),
    Pipe(Box<Pipeline>),
    /// Fields chained onto a function result or parenthesized pipeline.
    Chain(Box<Operand>, Vec<String>),
}

impl Pipeline {
    pub(super) fn new(position: usize, commands: Vec<Command>) -> Self {
        Self {
            position,
            decl: Vec::new(),
            assign: false,
            commands,
        }
    }

    pub(super) fn declare(mut self, decl: Vec<String>, assign: bool) -> Self {
        self.decl = decl;
        self.assign = assign;
        self
    }
}

impl std::fmt::Display for Operand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields = |names: &[String]| {
            names
                .iter()
                .map(|name| format!(".{name}"))
                .collect::<String>()
        };
        match self {
            Self::Nil => f.write_str("nil"),
            Self::Bool(value) => write!(f, "{value}"),
            Self::Int(value) => write!(f, "{value}"),
            Self::Float(value) => write!(f, "{value}"),
            Self::String(value) => write!(f, "{value:?}"),
            Self::Dot => f.write_str("."),
            Self::Field(names) => f.write_str(&fields(names)),
            Self::Variable(name, names) => write!(f, "{name}{}", fields(names)),
            Self::Function(name, _) => f.write_str(name),
            Self::Pipe(_) => f.write_str("(...)"),
            Self::Chain(operand, names) => write!(f, "{operand}{}", fields(names)),
        }
    }
}
