use logos::Logos;

use super::{ParseError, SourceSpan};

#[derive(Logos, Clone, Debug, Eq, PartialEq)]
#[logos(skip r"[ \t\r\n\f]+")]
pub(crate) enum Token {
    #[regex(r"#[^\n]*", logos::skip, allow_greedy = true)]
    Comment,
    #[token("&>>")]
    UnionDescendant,
    #[token("&<<")]
    UnionAncestor,
    #[token("!>>")]
    NotDescendant,
    #[token("!<<")]
    NotAncestor,
    #[token(">>")]
    Descendant,
    #[token("<<")]
    Ancestor,
    #[token("&>")]
    UnionChild,
    #[token("&<")]
    UnionParent,
    #[token("&~")]
    UnionSibling,
    #[token("!>")]
    NotChild,
    #[token("!<")]
    NotParent,
    #[token("&&")]
    And,
    #[token("||")]
    Or,
    #[token("=~")]
    Regex,
    #[token("!~")]
    NotRegex,
    #[token("!=")]
    NotEqual,
    #[token(">=")]
    GreaterEqual,
    #[token("<=")]
    LessEqual,
    #[token("resource.")]
    ResourceDot,
    #[token("span.")]
    SpanDot,
    #[token("trace:")]
    TraceColon,
    #[token("span:")]
    SpanColon,
    #[token("instrumentation.")]
    InstrumentationDot,
    #[token("instrumentation:")]
    InstrumentationColon,
    #[token("{")]
    LeftBrace,
    #[token("}")]
    RightBrace,
    #[token("(")]
    LeftParen,
    #[token(")")]
    RightParen,
    #[token(",")]
    Comma,
    #[token("|")]
    Pipe,
    #[token(".")]
    Dot,
    #[token("=")]
    Equal,
    #[token(">")]
    Greater,
    #[token("<")]
    Less,
    #[token("~")]
    Sibling,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,
    #[token("%")]
    Percent,
    #[token("^")]
    Caret,
    #[token("!")]
    Not,
    #[regex(r#""([^"\\]|\\.)*""#)]
    Quoted,
    #[regex(r"`[^`]*`")]
    RawString,
    #[regex(r"([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h|d|w|y))+", priority = 4)]
    Duration,
    #[regex(r"[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?", priority = 3)]
    Float,
    #[regex(r"[0-9]+")]
    Integer,
    #[regex(r"[A-Za-z_][A-Za-z0-9_-]*")]
    Ident,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Lexeme {
    pub token: Token,
    pub text: String,
    pub span: SourceSpan,
}

pub(crate) fn lex(source: &str) -> Result<Vec<Lexeme>, ParseError> {
    let mut lexer = Token::lexer(source);
    let mut output = Vec::new();
    while let Some(result) = lexer.next() {
        let range = lexer.span();
        let span = SourceSpan::new(range.start, range.end);
        let token = result.map_err(|()| {
            ParseError::new(
                format!("unexpected token `{}`", &source[range.clone()]),
                span,
            )
        })?;
        output.push(Lexeme {
            token,
            text: source[range].to_owned(),
            span,
        });
    }
    Ok(output)
}

pub(crate) fn decode_string(token: &Lexeme) -> Result<String, ParseError> {
    match token.token {
        Token::Quoted => serde_json::from_str(&escape_control_characters(&token.text))
            .map_err(|error| ParseError::new(format!("invalid string: {error}"), token.span)),
        Token::RawString => Ok(token.text[1..token.text.len() - 1].to_owned()),
        _ => Err(ParseError::new("expected string", token.span)),
    }
}

/// TraceQL allows raw control characters inside quotes; JSON does not.
fn escape_control_characters(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.chars().any(char::is_control) {
        return text.into();
    }
    let mut escaped = String::with_capacity(text.len() + 8);
    for character in text.chars() {
        match character {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", u32::from(character)));
            }
            character => escaped.push(character),
        }
    }
    escaped.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_strings_accept_raw_control_characters() {
        let tokens = lex("\"a\tb\nc\"").unwrap();
        assert_eq!(decode_string(&tokens[0]).unwrap(), "a\tb\nc");
    }

    #[test]
    fn lexes_longest_operators() {
        let tokens = lex(">> !>> &>> && || =~ !~").unwrap();
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            [
                Token::Descendant,
                Token::NotDescendant,
                Token::UnionDescendant,
                Token::And,
                Token::Or,
                Token::Regex,
                Token::NotRegex
            ]
        );
    }

    #[test]
    fn lexes_quoted_attributes_and_strings() {
        let tokens = lex(r#"{ resource."service name" = `api` }"#).unwrap();
        assert!(tokens.iter().any(|token| token.token == Token::ResourceDot));
        assert_eq!(decode_string(&tokens[2]).unwrap(), "service name");
        assert_eq!(decode_string(&tokens[4]).unwrap(), "api");
    }

    #[test]
    fn duration_is_one_token() {
        let tokens = lex("1h2m3.5s").unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].token, Token::Duration);
    }
}
