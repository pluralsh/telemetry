//! Go `text/template`, as Loki's `line_format` and `label_format` use it.
//!
//! [`lexer`] and the LALRPOP grammar follow `text/template/parse`;
//! [`resolve`] applies the parser's semantic checks (defined functions and
//! variables, `break` placement, template references) and [`exec`] follows
//! `text/template/exec` with Loki's `missingkey=zero` option: the data is the
//! row's label map, so a missing label renders empty.

mod ast;
mod exec;
mod lexer;
mod resolve;
lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all, clippy::pedantic)]
    grammar,
    "/query/template/go/grammar.rs"
);

use ast::Operand;
use lalrpop_util::ParseError;
use lexer::Tok;

use super::Context;
use crate::Result;

pub(super) struct Template {
    tree: resolve::Tree,
}

impl Template {
    pub(super) fn compile(source: &str) -> std::result::Result<Self, String> {
        let tokens = lexer::lex(source).map_err(|error| error.render(source))?;
        let items = grammar::TemplateParser::new()
            .parse(tokens.into_iter().map(Ok))
            .map_err(|error| TemplateError::from_parse(error, source.len()).render(source))?;
        let tree = resolve::resolve(items).map_err(|error| error.render(source))?;
        Ok(Self { tree })
    }

    pub(super) fn render(&self, context: &dyn Context) -> Result<String> {
        exec::execute(&self.tree, context)
    }
}

#[derive(Debug)]
pub(super) struct TemplateError {
    position: usize,
    message: String,
}

impl TemplateError {
    fn new(position: usize, message: impl Into<String>) -> Self {
        Self {
            position,
            message: message.into(),
        }
    }

    fn from_parse(error: ParseError<usize, Tok, TemplateError>, eof: usize) -> Self {
        match error {
            ParseError::User { error } => error,
            ParseError::InvalidToken { location } => Self::new(location, "invalid token"),
            ParseError::UnrecognizedEof { .. } => Self::new(eof, "unexpected EOF"),
            ParseError::UnrecognizedToken {
                token: (position, token, _),
                ..
            }
            | ParseError::ExtraToken {
                token: (position, token, _),
            } => Self::new(position, format!("unexpected {token}")),
        }
    }

    /// Go's `template: name:line:col: message`, without a template name.
    fn render(&self, source: &str) -> String {
        let before = &source[..self.position.min(source.len())];
        let line = before.matches('\n').count() + 1;
        let column = before.len() - before.rfind('\n').map_or(0, |index| index + 1) + 1;
        format!("template: {line}:{column}: {}", self.message)
    }
}

/// Folds fields chained onto a term, as Go's parser does: onto fields and
/// variables directly, onto function results and pipelines as a chain, and
/// never onto a literal.
fn chain(
    position: usize,
    term: Operand,
    fields: Vec<String>,
) -> std::result::Result<Operand, ParseError<usize, Tok, TemplateError>> {
    if fields.is_empty() {
        return Ok(term);
    }
    Ok(match term {
        Operand::Field(mut names) => {
            names.extend(fields);
            Operand::Field(names)
        }
        Operand::Variable(name, mut names) => {
            names.extend(fields);
            Operand::Variable(name, names)
        }
        Operand::Function(..) | Operand::Pipe(_) => Operand::Chain(Box::new(term), fields),
        literal => {
            return Err(ParseError::User {
                error: TemplateError::new(position, format!("unexpected . after term {literal}")),
            });
        }
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{TestRow, render};
    use crate::logql::TemplateSyntax;

    fn go(source: &str, labels: &[(&str, &str)]) -> String {
        render(TemplateSyntax::Go, source, &TestRow::new(labels))
            .unwrap_or_else(|error| panic!("{source}: {error}"))
    }

    fn go_err(source: &str, labels: &[(&str, &str)]) -> String {
        match render(TemplateSyntax::Go, source, &TestRow::new(labels)) {
            Ok(output) => panic!("{source} rendered {output:?}"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn fields_pipelines_and_missing_labels() {
        let labels = [("level", "error"), ("user", "alice"), ("value", "10")];
        assert_eq!(go("{{.user}} {{.missing}}!", &labels), "alice !");
        assert_eq!(go("{{ .user | upper | repeat 2 }}", &labels), "ALICEALICE");
        assert_eq!(go("{{ .value | add 2 }}", &labels), "12");
        assert_eq!(
            go("{{ __line__ }} {{ __timestamp__ | unixEpoch }}", &labels),
            "the line 1500000000"
        );
        assert_eq!(
            go(r#"{{ printf "%-6s|%3d" .user (len .level) }}"#, &labels),
            "alice |  5"
        );
        assert_eq!(go("{{ (.user | upper) }}", &labels), "ALICE");
        assert_eq!(
            go(r#"{{ index . "user" }} {{ len . }}"#, &labels),
            "alice 3"
        );
    }

    #[test]
    fn control_structures() {
        let labels = [("level", "error"), ("n", "3"), ("empty", "")];
        assert_eq!(
            go(
                r#"{{if eq .level "info"}}i{{else if eq .level "error"}}e{{else}}?{{end}}"#,
                &labels
            ),
            "e"
        );
        assert_eq!(
            go(
                "{{with .level}}[{{.}}]{{end}}{{with .empty}}x{{else}}none{{end}}",
                &labels
            ),
            "[error]none"
        );
        assert_eq!(
            go("{{range $k, $v := .}}{{$k}}={{$v}};{{end}}", &labels),
            "empty=;level=error;n=3;"
        );
        assert_eq!(
            go(
                "{{range $i := 4}}{{if eq $i 2}}{{continue}}{{end}}{{$i}}{{end}}",
                &labels
            ),
            "013"
        );
        assert_eq!(
            go(
                "{{range 10}}{{if ge . 2}}{{break}}{{end}}{{.}}{{end}}",
                &labels
            ),
            "01"
        );
        assert_eq!(
            go(
                r#"{{range $x := fromJson "[]"}}x{{else}}empty{{end}}"#,
                &labels
            ),
            "empty"
        );
        assert_eq!(
            go(
                r#"{{with $x := "a"}}{{$x}}{{else with $y := "b"}}{{$y}}{{end}}"#,
                &labels
            ),
            "a"
        );
        assert_eq!(
            go(r#"{{$x := 1}}{{if true}}{{$x = 2}}{{end}}{{$x}}"#, &labels),
            "2"
        );
    }

    #[test]
    fn whitespace_trimming_and_comments() {
        assert_eq!(
            go("a \n {{- .x -}} \n b{{/* gone */}}c", &[("x", "X")]),
            "aXbc"
        );
    }

    #[test]
    fn builtins() {
        let labels = [("a", "x"), ("n", "5")];
        assert_eq!(
            go(
                r#"{{ and .a .missing "z" }}|{{ or .missing .a }}|{{ not .a }}"#,
                &labels
            ),
            "|x|false"
        );
        assert_eq!(
            go(
                r#"{{ eq .a "y" "x" }} {{ ne 1 2 }} {{ lt 1 2 }} {{ ge "b" "a" }}"#,
                &labels
            ),
            "true true true true"
        );
        assert_eq!(
            go(r#"{{ print 1 2 "s" }}{{ println "a" 1 }}"#, &labels),
            "1 2sa 1\n"
        );
        assert_eq!(
            go(
                r#"{{ html "<a href='x'>" }} {{ js "it's" }} {{ urlquery "a b" }}"#,
                &labels
            ),
            "&lt;a href=&#39;x&#39;&gt; it\\'s a+b"
        );
        assert_eq!(
            go(r#"{{ slice "abcdef" 1 3 }} {{ index "abc" 1 }}"#, &labels),
            "bc 98"
        );
        assert_eq!(
            go(r#"{{ (fromJson "{\"a\":{\"b\":[1,2]}}").a.b }}"#, &labels),
            "[1 2]"
        );
        assert_eq!(
            go(r#"{{ index (fromJson "[\"p\",\"q\"]") 1 }}"#, &labels),
            "q"
        );
        assert_eq!(go(r#"{{ and false (div 1 0) }}"#, &labels), "false");
    }

    #[test]
    fn define_template_and_block() {
        assert_eq!(
            go(
                r#"{{define "t"}}<{{.}}>{{end}}{{template "t" .a}}{{block "b" "q"}}[{{.}}]{{end}}"#,
                &[("a", "x")]
            ),
            "<x>[q]"
        );
        assert!(
            go_err(
                r#"{{define "r"}}{{template "r"}}{{end}}{{template "r"}}"#,
                &[]
            )
            .contains("depth")
        );
    }

    #[test]
    fn parse_time_errors() {
        assert!(go_err("{{ nosuchfunc }}", &[]).contains(r#"function "nosuchfunc" not defined"#));
        assert!(go_err("{{ .a | nosuchfunc }}", &[]).contains("nosuchfunc"));
        assert!(go_err("{{ upper (nosuchfunc .a) }}", &[]).contains("nosuchfunc"));
        assert!(go_err("{{ $x }}", &[]).contains(r#"undefined variable "$x""#));
        assert!(go_err("{{ $x = 1 }}", &[]).contains("undefined variable"));
        assert!(go_err("{{ break }}", &[]).contains("break"));
        assert!(go_err("{{ if .a }}x", &[]).contains("unexpected EOF"));
        assert!(go_err("{{ end }}", &[]).contains("unexpected <end>"));
        assert!(go_err("{{ .a | 1 }}", &[]).contains("non executable command"));
        assert!(go_err(r#"{{ template "nope" }}"#, &[]).contains("no such template"));
        assert!(go_err("{{ \"a\".b }}", &[]).contains("unexpected . after term"));
        assert!(
            go_err("a\n {{ bad }}", &[]).ends_with(r#"template: 2:5: function "bad" not defined"#)
        );
    }

    #[test]
    fn execution_errors() {
        assert!(
            go_err(r#"{{ eq .a 1 }}"#, &[("a", "1")]).contains("incompatible types for comparison")
        );
        assert!(go_err(r#"{{ .a.b }}"#, &[("a", "x")]).contains("can't evaluate field b"));
        assert!(
            go_err(r#"{{ "x" | printf "%s" | .a }}"#, &[])
                .contains("can't give argument to non-function")
        );
        assert!(
            go_err("{{ range .a }}{{ end }}", &[("a", "x")]).contains("range can't iterate over")
        );
        assert!(go_err("{{ range 1000000 }}{{ end }}", &[]).contains("budget"));
    }
}
