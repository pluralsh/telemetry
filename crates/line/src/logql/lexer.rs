// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");

use logos::Logos;

use super::ast::Span;
use super::error::ParseError;

#[derive(Logos, Clone, Debug, PartialEq)]
#[logos(skip r"[ \t\r\n\f]+")]
pub(crate) enum Token {
    #[regex(r"#[^\n]*", logos::skip, allow_greedy = true)]
    Comment,
    #[token("|>")]
    PipePattern,
    #[token("!>")]
    NotPattern,
    #[token("|=")]
    PipeEqual,
    #[token("|~")]
    PipeRegex,
    #[token("=~")]
    RegexEqual,
    #[token("!~")]
    NotRegex,
    #[token("!=")]
    NotEqual,
    #[token("==")]
    EqualEqual,
    #[token(">=")]
    GreaterEqual,
    #[token("<=")]
    LessEqual,
    #[token("|")]
    Pipe,
    #[token("=")]
    Equal,
    #[token(">")]
    Greater,
    #[token("<")]
    Less,
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
    #[token("{")]
    LeftBrace,
    #[token("}")]
    RightBrace,
    #[token("(")]
    LeftParen,
    #[token(")")]
    RightParen,
    #[token("[")]
    LeftBracket,
    #[token("]")]
    RightBracket,
    #[token(",")]
    Comma,
    #[regex(r#""([^"\\\n]|\\.)*""#)]
    Quoted,
    #[regex(r"`[^`]*`")]
    RawString,
    #[regex(r"--[A-Za-z][A-Za-z0-9-]*")]
    Flag,
    #[regex(r"-?([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h|d|w|y))+", priority = 3)]
    #[regex(r"[0-9]+(\.[0-9]+)?([kKmMgGtTpPeE]i?)?[bB]", priority = 3)]
    Quantity,
    #[regex(r"0[xX][0-9A-Fa-f_]+(\.[0-9A-Fa-f_]*)?[pP][+-]?[0-9_]+", priority = 4)]
    #[regex(r"([0-9][0-9_]*(\.[0-9_]*)?|\.[0-9_]+)([eE][+-]?[0-9_]+)?")]
    Number,
    #[regex(r"[A-Za-z_][A-Za-z0-9_:.-]*")]
    Ident,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Lexeme {
    pub token: Token,
    pub text: String,
    pub span: Span,
}

pub(crate) fn lex(source: &str) -> Result<Vec<Lexeme>, ParseError> {
    let mut lexer = Token::lexer(source);
    let mut tokens = Vec::new();
    while let Some(result) = lexer.next() {
        let range = lexer.span();
        let span = Span::new(range.start, range.end);
        let token = result.map_err(|()| {
            ParseError::new(
                format!("unexpected token `{}`", &source[range.clone()]),
                span,
            )
        })?;
        tokens.push(Lexeme {
            token,
            text: source[range].to_owned(),
            span,
        });
    }
    Ok(tokens)
}

pub(crate) fn decode_string(lexeme: &Lexeme) -> Result<String, ParseError> {
    match lexeme.token {
        Token::Quoted => serde_json::from_str(&lexeme.text)
            .map_err(|error| ParseError::new(format!("invalid string: {error}"), lexeme.span)),
        Token::RawString => Ok(lexeme.text[1..lexeme.text.len() - 1].to_owned()),
        _ => Err(ParseError::new("expected string", lexeme.span)),
    }
}
