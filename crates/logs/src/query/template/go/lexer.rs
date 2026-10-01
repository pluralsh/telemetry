//! Go `text/template` lexing (`text/template/parse/lex.go`).
//!
//! The lexer is modal: text runs to the next `{{`, honouring `{{- ` and
//! ` -}}` trim markers and `{{/* */}}` comments, and Logos tokenizes each
//! action. Spaces only matter between operands: a field directly after an
//! operand chains onto it (`$x.a`, `(f).a`), and any other adjacent operands
//! are rejected, so the token stream the grammar sees carries no spaces.

use std::fmt;

use logos::Logos;

use super::TemplateError;

const SPACE_CHARS: &[char] = &[' ', '\t', '\r', '\n'];

#[derive(Logos, Clone, Copy, Debug, PartialEq)]
enum Raw {
    #[regex(r"[ \t\r\n]+")]
    Space,
    #[token("}}")]
    Close,
    #[token("-}}")]
    TrimClose,
    #[token("|")]
    Pipe,
    #[token("(")]
    LeftParen,
    #[token(")")]
    RightParen,
    #[token(":=")]
    Declare,
    #[token("=")]
    Assign,
    #[token(",")]
    Comma,
    #[regex(r#""([^"\\\n]|\\.)*""#)]
    Quoted,
    #[regex(r"`[^`]*`")]
    Backquoted,
    #[regex(r"'([^'\\\n]|\\.)+'")]
    Char,
    #[regex(
        r"[+-]?(0[xX][0-9a-fA-F_]+|0[bB][01_]+|0[oO][0-7_]+|([0-9][0-9_]*(\.[0-9_]*)?|\.[0-9][0-9_]*)([eE][+-]?[0-9_]+)?)"
    )]
    Number,
    #[regex(r"\.[A-Za-z_][A-Za-z0-9_]*")]
    Field,
    #[token(".")]
    Dot,
    #[regex(r"\$[A-Za-z0-9_]*")]
    Variable,
    #[regex(r"[A-Za-z_][A-Za-z0-9_]*")]
    Ident,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Tok {
    Text(String),
    Open,
    Close,
    If,
    Else,
    End,
    Range,
    With,
    Define,
    Template,
    Block,
    Break,
    Continue,
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Field(String),
    Chain(String),
    Dot,
    Variable(String),
    Ident(String),
    Pipe,
    LeftParen,
    RightParen,
    Declare,
    Assign,
    Comma,
}

impl Tok {
    fn starts_operand(&self) -> bool {
        matches!(
            self,
            Self::Nil
                | Self::Bool(_)
                | Self::Int(_)
                | Self::Float(_)
                | Self::Str(_)
                | Self::Field(_)
                | Self::Dot
                | Self::Variable(_)
                | Self::Ident(_)
                | Self::LeftParen
        )
    }

    fn ends_operand(&self) -> bool {
        matches!(
            self,
            Self::Nil
                | Self::Bool(_)
                | Self::Int(_)
                | Self::Float(_)
                | Self::Str(_)
                | Self::Field(_)
                | Self::Chain(_)
                | Self::Dot
                | Self::Variable(_)
                | Self::Ident(_)
                | Self::RightParen
        )
    }
}

impl fmt::Display for Tok {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => write!(f, "text {text:?}"),
            Self::Open => f.write_str("\"{{\""),
            Self::Close => f.write_str("\"}}\""),
            Self::If => f.write_str("<if>"),
            Self::Else => f.write_str("<else>"),
            Self::End => f.write_str("<end>"),
            Self::Range => f.write_str("<range>"),
            Self::With => f.write_str("<with>"),
            Self::Define => f.write_str("<define>"),
            Self::Template => f.write_str("<template>"),
            Self::Block => f.write_str("<block>"),
            Self::Break => f.write_str("<break>"),
            Self::Continue => f.write_str("<continue>"),
            Self::Nil => f.write_str("<nil>"),
            Self::Bool(value) => write!(f, "{value:?}"),
            Self::Int(value) => write!(f, "{value:?}"),
            Self::Float(value) => write!(f, "{value:?}"),
            Self::Str(value) => write!(f, "{value:?}"),
            Self::Field(name) | Self::Chain(name) => write!(f, "\".{name}\""),
            Self::Dot => f.write_str("\".\""),
            Self::Variable(name) | Self::Ident(name) => write!(f, "{name:?}"),
            Self::Pipe => f.write_str("\"|\""),
            Self::LeftParen => f.write_str("\"(\""),
            Self::RightParen => f.write_str("\")\""),
            Self::Declare => f.write_str("\":=\""),
            Self::Assign => f.write_str("\"=\""),
            Self::Comma => f.write_str("\",\""),
        }
    }
}

pub(super) type Spanned = (usize, Tok, usize);

pub(super) fn lex(source: &str) -> Result<Vec<Spanned>, TemplateError> {
    let mut tokens = Vec::new();
    let mut position = 0;
    let mut trim_text_start = false;
    loop {
        let open = source[position..].find("{{").map(|index| position + index);
        let text_end = open.unwrap_or(source.len());
        let mut text = &source[position..text_end];
        if trim_text_start {
            text = text.trim_start_matches(SPACE_CHARS);
        }
        let mut action = text_end + 2;
        if open.is_some() && has_left_trim(&source[action..]) {
            text = text.trim_end_matches(SPACE_CHARS);
            action += 2;
        }
        if !text.is_empty() {
            tokens.push((position, Tok::Text(text.to_owned()), text_end));
        }
        let Some(open) = open else {
            break;
        };
        if source[action..].starts_with("/*") {
            let end = source[action..]
                .find("*/")
                .map(|end| action + end + 2)
                .ok_or_else(|| TemplateError::new(open, "unclosed comment"))?;
            let (close_end, trim) = comment_close(&source[end..])
                .ok_or_else(|| TemplateError::new(end, "comment ends before closing delimiter"))?;
            position = end + close_end;
            trim_text_start = trim;
            continue;
        }
        tokens.push((open, Tok::Open, action));
        let (end, trim) = lex_action(source, open, action, &mut tokens)?;
        position = end;
        trim_text_start = trim;
    }
    Ok(tokens)
}

/// `{{- ` trims: the dash must be followed by a space character.
fn has_left_trim(rest: &str) -> bool {
    let mut chars = rest.chars();
    chars.next() == Some('-') && chars.next().is_some_and(|c| SPACE_CHARS.contains(&c))
}

/// A comment must close with `}}` or ` -}}` immediately after `*/`.
fn comment_close(rest: &str) -> Option<(usize, bool)> {
    if rest.starts_with("}}") {
        return Some((2, false));
    }
    let mut chars = rest.chars();
    let space = chars.next().filter(|c| SPACE_CHARS.contains(c))?;
    chars
        .as_str()
        .starts_with("-}}")
        .then(|| (space.len_utf8() + 3, true))
}

/// Lexes one action, returning the offset just past its closing delimiter
/// and whether that delimiter trims the following text.
fn lex_action(
    source: &str,
    open: usize,
    start: usize,
    tokens: &mut Vec<Spanned>,
) -> Result<(usize, bool), TemplateError> {
    let mut lexer = Raw::lexer(&source[start..]);
    let mut spaced = true;
    let mut previous_ends_operand = false;
    while let Some(raw) = lexer.next() {
        let range = lexer.span();
        let (from, to) = (start + range.start, start + range.end);
        let text = &source[from..to];
        let raw =
            raw.map_err(|()| TemplateError::new(from, format!("unexpected {text:?} in command")))?;
        let token = match raw {
            Raw::Space => {
                spaced = true;
                continue;
            }
            Raw::Close => {
                tokens.push((from, Tok::Close, to));
                return Ok((to, false));
            }
            Raw::TrimClose if spaced => {
                tokens.push((from, Tok::Close, to));
                return Ok((to, true));
            }
            Raw::TrimClose => {
                return Err(TemplateError::new(from, "unexpected \"-\" in command"));
            }
            Raw::Pipe => Tok::Pipe,
            Raw::LeftParen => Tok::LeftParen,
            Raw::RightParen => Tok::RightParen,
            Raw::Declare => Tok::Declare,
            Raw::Assign => Tok::Assign,
            Raw::Comma => Tok::Comma,
            Raw::Quoted => Tok::Str(
                go_unquote(&text[1..text.len() - 1])
                    .ok_or_else(|| TemplateError::new(from, format!("invalid string {text}")))?,
            ),
            Raw::Backquoted => Tok::Str(text[1..text.len() - 1].replace('\r', "")),
            Raw::Char => char_constant(text).ok_or_else(|| {
                TemplateError::new(from, format!("malformed character constant {text}"))
            })?,
            Raw::Number => number(text)
                .ok_or_else(|| TemplateError::new(from, format!("bad number syntax: {text:?}")))?,
            Raw::Field if !spaced && previous_ends_operand => Tok::Chain(text[1..].to_owned()),
            Raw::Field => Tok::Field(text[1..].to_owned()),
            Raw::Dot => Tok::Dot,
            Raw::Variable => Tok::Variable(text.to_owned()),
            Raw::Ident => keyword(text),
        };
        if !spaced && previous_ends_operand && token.starts_operand() {
            return Err(TemplateError::new(
                from,
                format!("unexpected {token} in operand"),
            ));
        }
        previous_ends_operand = token.ends_operand();
        spaced = false;
        tokens.push((from, token, to));
    }
    Err(TemplateError::new(open, "unclosed action"))
}

fn keyword(text: &str) -> Tok {
    match text {
        "if" => Tok::If,
        "else" => Tok::Else,
        "end" => Tok::End,
        "range" => Tok::Range,
        "with" => Tok::With,
        "define" => Tok::Define,
        "template" => Tok::Template,
        "block" => Tok::Block,
        "break" => Tok::Break,
        "continue" => Tok::Continue,
        "nil" => Tok::Nil,
        "true" => Tok::Bool(true),
        "false" => Tok::Bool(false),
        _ => Tok::Ident(text.to_owned()),
    }
}

/// Go numeric literals: integers in any base (with `_` separators) stay
/// integers; anything with a fraction or exponent is a float.
fn number(text: &str) -> Option<Tok> {
    let clean = text.replace('_', "");
    let (negative, digits) = match clean.as_bytes().first()? {
        b'-' => (true, &clean[1..]),
        b'+' => (false, &clean[1..]),
        _ => (false, clean.as_str()),
    };
    let radix = |prefix: &[&str], radix: u32| {
        prefix
            .iter()
            .find_map(|prefix| digits.strip_prefix(prefix))
            .map(|body| i64::from_str_radix(body, radix))
    };
    let integer = radix(&["0x", "0X"], 16)
        .or_else(|| radix(&["0b", "0B"], 2))
        .or_else(|| radix(&["0o", "0O"], 8));
    let value = if let Some(integer) = integer {
        Tok::Int(integer.ok()?)
    } else if digits.contains(['.', 'e', 'E']) {
        Tok::Float(digits.parse().ok()?)
    } else if digits.len() > 1 && digits.starts_with('0') {
        Tok::Int(i64::from_str_radix(&digits[1..], 8).ok()?)
    } else {
        Tok::Int(digits.parse().ok()?)
    };
    Some(match value {
        Tok::Int(value) if negative => Tok::Int(-value),
        Tok::Float(value) if negative => Tok::Float(-value),
        value => value,
    })
}

fn char_constant(text: &str) -> Option<Tok> {
    let decoded = go_unquote(&text[1..text.len() - 1])?;
    let mut chars = decoded.chars();
    let character = chars.next()?;
    chars
        .next()
        .is_none()
        .then(|| Tok::Int(i64::from(u32::from(character))))
}

/// Decodes the body of a Go interpreted string literal: JSON's escapes plus
/// `\a`, `\v`, `\'`, `\xHH`, octal `\ooo` and `\UXXXXXXXX`.
fn go_unquote(body: &str) -> Option<String> {
    let mut output = Vec::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            let mut buffer = [0; 4];
            output.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            continue;
        }
        let escaped = chars.next()?;
        let mut digits = |count: usize, radix: u32| -> Option<u32> {
            let text: String = chars.by_ref().take(count).collect();
            (text.len() == count)
                .then(|| u32::from_str_radix(&text, radix).ok())
                .flatten()
        };
        match escaped {
            'a' => output.push(0x07),
            'b' => output.push(0x08),
            'f' => output.push(0x0c),
            'n' => output.push(b'\n'),
            'r' => output.push(b'\r'),
            't' => output.push(b'\t'),
            'v' => output.push(0x0b),
            '\\' | '"' | '\'' => output.push(escaped as u8),
            'x' => output.push(digits(2, 16)? as u8),
            '0'..='7' => {
                let rest = digits(2, 8)?;
                let value = escaped.to_digit(8)? * 64 + rest;
                output.push(u8::try_from(value).ok()?);
            }
            'u' | 'U' => {
                let value = digits(if escaped == 'u' { 4 } else { 8 }, 16)?;
                let mut buffer = [0; 4];
                output
                    .extend_from_slice(char::from_u32(value)?.encode_utf8(&mut buffer).as_bytes());
            }
            _ => return None,
        }
    }
    Some(String::from_utf8_lossy(&output).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(source: &str) -> Vec<Tok> {
        lex(source)
            .unwrap()
            .into_iter()
            .map(|(_, token, _)| token)
            .collect()
    }

    #[test]
    fn text_and_actions() {
        assert_eq!(
            kinds(r#"a {{ printf "%s}}" .b.c }} z"#),
            vec![
                Tok::Text("a ".into()),
                Tok::Open,
                Tok::Ident("printf".into()),
                Tok::Str("%s}}".into()),
                Tok::Field("b".into()),
                Tok::Chain("c".into()),
                Tok::Close,
                Tok::Text(" z".into()),
            ]
        );
    }

    #[test]
    fn trim_markers_and_comments() {
        assert_eq!(
            kinds("a  {{- .x -}}  b {{/* note */}}c{{- /* x */ -}}  d"),
            vec![
                Tok::Text("a".into()),
                Tok::Open,
                Tok::Field("x".into()),
                Tok::Close,
                Tok::Text("b ".into()),
                Tok::Text("c".into()),
                Tok::Text("d".into()),
            ]
        );
        assert_eq!(kinds("{{-3}}")[1], Tok::Int(-3));
        assert!(lex("{{ .x-}}").is_err());
        assert!(lex("{{/* x */ }}").is_err());
    }

    #[test]
    fn literals() {
        assert_eq!(
            kinds(r"{{ 0x1F 0o17 017 1_000 1.5 1e3 'a' '\n' `raw\n` true nil $ $x }}")[1..14],
            [
                Tok::Int(31),
                Tok::Int(15),
                Tok::Int(15),
                Tok::Int(1000),
                Tok::Float(1.5),
                Tok::Float(1000.0),
                Tok::Int(97),
                Tok::Int(10),
                Tok::Str(r"raw\n".into()),
                Tok::Bool(true),
                Tok::Nil,
                Tok::Variable("$".into()),
                Tok::Variable("$x".into()),
            ]
        );
        assert_eq!(
            go_unquote(r"\x1b[0m\t\u00e9\101").unwrap(),
            "\x1b[0m\té\x41"
        );
    }

    #[test]
    fn chains_need_adjacency_and_operands_need_spaces() {
        assert_eq!(kinds("{{ (f).a }}")[4], Tok::Chain("a".into()));
        assert_eq!(kinds("{{ (f) .a }}")[4], Tok::Field("a".into()));
        assert!(lex(r#"{{ "a""b" }}"#).is_err());
        assert!(lex("{{ .a(x) }}").is_err());
        assert!(lex("{{ .a").is_err());
    }
}
