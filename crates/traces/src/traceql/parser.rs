use super::ast::{
    AggregateOp, Attribute, AttributeScope, BinaryOp, Expr, FieldExpr, Hint, Intrinsic, KindValue,
    PipelineStage, Query, ScalarExpr, SourceSpan, Spanned, SpansetExpr, StaticValue, StatusValue,
    StructuralOp, UnaryOp,
};
use super::error::ParseError;
use super::grammar;
use super::lexer::{Lexeme, Token, decode_string, lex};

#[derive(Clone, Debug)]
pub(crate) enum ExprToken {
    Operand(FieldExpr),
    Or(SourceSpan),
    And(SourceSpan),
    Equal(SourceSpan),
    NotEqual(SourceSpan),
    Regex(SourceSpan),
    NotRegex(SourceSpan),
    Less(SourceSpan),
    LessEqual(SourceSpan),
    Greater(SourceSpan),
    GreaterEqual(SourceSpan),
    Plus(SourceSpan),
    Minus(SourceSpan),
    Star(SourceSpan),
    Slash(SourceSpan),
    Percent(SourceSpan),
    Caret(SourceSpan),
    Not(SourceSpan),
    Neg(SourceSpan),
    LeftParen(SourceSpan),
    RightParen(SourceSpan),
}

pub(crate) fn combine_binary(
    lhs: FieldExpr,
    op: BinaryOp,
    _operator: SourceSpan,
    rhs: FieldExpr,
) -> FieldExpr {
    let span = lhs.span.join(rhs.span);
    Spanned::new(
        Expr::Binary {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        },
        span,
    )
}

pub(crate) fn combine_unary(op: UnaryOp, operator: SourceSpan, expr: FieldExpr) -> FieldExpr {
    let span = operator.join(expr.span);
    Spanned::new(
        Expr::Unary {
            op,
            expr: Box::new(expr),
        },
        span,
    )
}

pub(crate) fn parse_syntax(source: &str) -> Result<Query, ParseError> {
    let tokens = lex(source)?;
    if tokens.is_empty() {
        return Err(ParseError::new(
            "query cannot be empty",
            SourceSpan::new(0, 0),
        ));
    }
    let mut parser = Parser {
        source,
        tokens,
        position: 0,
    };
    let mut stages = Vec::new();
    // A pipeline may open with a stage other than a spanset expression; it
    // then applies to every span of the trace.
    let spanset = if parser.starts_leading_stage() {
        stages.push(parser.parse_stage()?);
        Spanned::new(
            SpansetExpr::Filter(Spanned::new(
                Expr::Static(StaticValue::Bool(true)),
                SourceSpan::new(0, 0),
            )),
            SourceSpan::new(0, 0),
        )
    } else {
        parser.parse_spanset(0)?
    };
    while parser.eat_token(Token::Pipe).is_some() {
        stages.push(parser.parse_stage()?);
    }
    let hints = if parser.eat_keyword("with").is_some() {
        parser.parse_hints()?
    } else {
        Vec::new()
    };
    if let Some(token) = parser.peek() {
        return Err(ParseError::new(
            format!("unexpected token `{}`", token.text),
            token.span,
        ));
    }
    Ok(Query {
        spanset,
        stages,
        hints,
    })
}

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Lexeme>,
    position: usize,
}

impl Parser<'_> {
    fn parse_spanset(
        &mut self,
        minimum_precedence: u8,
    ) -> Result<Spanned<SpansetExpr>, ParseError> {
        let mut lhs = if let Some(left) = self.eat_token(Token::LeftParen) {
            let mut inner = self.parse_spanset(0)?;
            let right = self.expect(Token::RightParen, "expected `)`")?;
            inner.span = left.span.join(right.span);
            inner
        } else {
            self.parse_filter()?
        };
        while let Some((op, precedence)) = self.peek_structural_op() {
            if precedence < minimum_precedence {
                break;
            }
            self.next();
            let rhs = self.parse_spanset(precedence + 1)?;
            let span = lhs.span.join(rhs.span);
            lhs = Spanned::new(
                SpansetExpr::Binary {
                    lhs: Box::new(lhs),
                    op,
                    rhs: Box::new(rhs),
                },
                span,
            );
        }
        Ok(lhs)
    }

    fn parse_filter(&mut self) -> Result<Spanned<SpansetExpr>, ParseError> {
        let left = self.expect(Token::LeftBrace, "expected `{`")?;
        if let Some(right) = self.eat_token(Token::RightBrace) {
            let expression = Spanned::new(
                Expr::Static(StaticValue::Bool(true)),
                left.span.join(right.span),
            );
            return Ok(Spanned::new(
                SpansetExpr::Filter(expression),
                left.span.join(right.span),
            ));
        }
        let start = self.position;
        let mut depth = 0_usize;
        let end = loop {
            let token = self.peek().ok_or_else(|| self.eof_error("expected `}`"))?;
            match token.token {
                Token::LeftParen => depth += 1,
                Token::RightParen if depth > 0 => depth -= 1,
                Token::RightBrace if depth == 0 => break self.position,
                _ => {}
            }
            self.position += 1;
        };
        let expression = parse_field_tokens(&self.tokens[start..end])?;
        let right = self.expect(Token::RightBrace, "expected `}`")?;
        Ok(Spanned::new(
            SpansetExpr::Filter(expression),
            left.span.join(right.span),
        ))
    }

    fn starts_leading_stage(&self) -> bool {
        self.peek().is_some_and(|token| {
            token.token == Token::Ident
                && (matches!(token.text.as_str(), "by" | "select")
                    || aggregate_op(&token.text).is_some())
        })
    }

    fn parse_stage(&mut self) -> Result<PipelineStage, ParseError> {
        if self
            .peek()
            .is_some_and(|token| token.token == Token::LeftBrace)
        {
            let filter = self.parse_filter()?;
            let SpansetExpr::Filter(expression) = filter.value else {
                unreachable!("parse_filter returns a filter");
            };
            return Ok(PipelineStage::SpansetFilter(expression));
        }
        let name = self.expect(Token::Ident, "expected pipeline stage")?;
        match name.text.as_str() {
            "by" => {
                let field = self.parse_parenthesized_field()?;
                Ok(PipelineStage::By(field))
            }
            "coalesce" => {
                self.expect(Token::LeftParen, "expected `(` after coalesce")?;
                self.expect(Token::RightParen, "expected `)` after coalesce")?;
                Ok(PipelineStage::Coalesce)
            }
            "select" => {
                self.expect(Token::LeftParen, "expected `(` after select")?;
                let mut fields = Vec::new();
                loop {
                    fields.push(self.parse_field_until(&[Token::Comma, Token::RightParen])?);
                    if self.eat_token(Token::Comma).is_none() {
                        break;
                    }
                }
                self.expect(Token::RightParen, "expected `)` after select")?;
                Ok(PipelineStage::Select(fields))
            }
            name_text if is_metric_name(name_text) => {
                let start = name.span.start;
                self.consume_balanced_call()?;
                let end = self.previous_span().end;
                Ok(PipelineStage::Metric {
                    name: name.text,
                    source: self.source[start..end].to_owned(),
                    span: SourceSpan::new(start, end),
                })
            }
            "count" | "min" | "max" | "avg" | "sum" => {
                self.position -= 1;
                self.parse_scalar_filter()
            }
            _ => Err(ParseError::new(
                format!("unknown pipeline stage `{}`", name.text),
                name.span,
            )),
        }
    }

    fn parse_scalar_filter(&mut self) -> Result<PipelineStage, ParseError> {
        let start = self.peek().expect("scalar stage has token").span.start;
        let lhs = self.parse_scalar_expr(0)?;
        let operator = self.next_owned("expected comparison after aggregate expression")?;
        let op = comparison_op(&operator).ok_or_else(|| {
            ParseError::new(
                "expected comparison after aggregate expression",
                operator.span,
            )
        })?;
        let rhs = self.parse_scalar_expr(0)?;
        let end = self.previous_span().end;
        Ok(PipelineStage::ScalarFilter {
            lhs,
            op,
            rhs,
            span: SourceSpan::new(start, end),
        })
    }

    fn parse_scalar_expr(&mut self, minimum_precedence: u8) -> Result<ScalarExpr, ParseError> {
        let mut lhs = self.parse_scalar_primary()?;
        while let Some((op, precedence, right_associative)) = self.peek_scalar_op() {
            if precedence < minimum_precedence {
                break;
            }
            self.next();
            let rhs = self.parse_scalar_expr(precedence + u8::from(!right_associative))?;
            lhs = ScalarExpr::Binary {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_scalar_primary(&mut self) -> Result<ScalarExpr, ParseError> {
        if self.eat_token(Token::Minus).is_some() {
            let value = self.parse_scalar_primary()?;
            return Ok(ScalarExpr::Binary {
                lhs: Box::new(ScalarExpr::Static(StaticValue::Int(0))),
                op: BinaryOp::Sub,
                rhs: Box::new(value),
            });
        }
        if self.eat_token(Token::LeftParen).is_some() {
            let value = self.parse_scalar_expr(0)?;
            self.expect(Token::RightParen, "expected `)`")?;
            return Ok(value);
        }
        let token = self.next_owned("expected scalar expression")?;
        if token.token == Token::Ident
            && let Some(op) = aggregate_op(&token.text)
        {
            self.expect(Token::LeftParen, "expected `(` after aggregate")?;
            let field = if let Some(right) = self.eat_token(Token::RightParen) {
                if op != AggregateOp::Count {
                    return Err(ParseError::new(
                        format!("`{}` requires a field", token.text),
                        token.span.join(right.span),
                    ));
                }
                None
            } else {
                let field = self.parse_field_until(&[Token::RightParen])?;
                self.expect(Token::RightParen, "expected `)` after aggregate")?;
                Some(field)
            };
            return Ok(ScalarExpr::Aggregate { op, field });
        }
        static_from_token(&token).map(ScalarExpr::Static)
    }

    fn parse_parenthesized_field(&mut self) -> Result<FieldExpr, ParseError> {
        self.expect(Token::LeftParen, "expected `(`")?;
        let field = self.parse_field_until(&[Token::RightParen])?;
        self.expect(Token::RightParen, "expected `)`")?;
        Ok(field)
    }

    fn parse_field_until(&mut self, delimiters: &[Token]) -> Result<FieldExpr, ParseError> {
        let start = self.position;
        let mut depth = 0_usize;
        while let Some(token) = self.peek() {
            if depth == 0 && delimiters.contains(&token.token) {
                break;
            }
            match token.token {
                Token::LeftParen => depth += 1,
                Token::RightParen if depth > 0 => depth -= 1,
                _ => {}
            }
            self.position += 1;
        }
        if start == self.position {
            return Err(self.eof_error("expected field expression"));
        }
        parse_field_tokens(&self.tokens[start..self.position])
    }

    fn parse_hints(&mut self) -> Result<Vec<Hint>, ParseError> {
        self.expect(Token::LeftParen, "expected `(` after with")?;
        let mut hints = Vec::new();
        loop {
            let name = self.expect(Token::Ident, "expected hint name")?;
            self.expect(Token::Equal, "expected `=` after hint name")?;
            let value = self.next_owned("expected hint value")?;
            let static_value = static_from_token(&value)?;
            hints.push(Hint {
                name: name.text,
                value: static_value,
                span: name.span.join(value.span),
            });
            if self.eat_token(Token::Comma).is_none() {
                break;
            }
        }
        self.expect(Token::RightParen, "expected `)` after hints")?;
        Ok(hints)
    }

    fn consume_balanced_call(&mut self) -> Result<(), ParseError> {
        self.expect(Token::LeftParen, "expected `(` after metric stage")?;
        let mut depth = 1_usize;
        while depth > 0 {
            let token = self.next_owned("expected `)` after metric stage")?;
            match token.token {
                Token::LeftParen => depth += 1,
                Token::RightParen => depth -= 1,
                _ => {}
            }
        }
        Ok(())
    }

    fn peek_structural_op(&self) -> Option<(StructuralOp, u8)> {
        let token = self.peek()?;
        Some(match token.token {
            Token::Or => (StructuralOp::Union, 1),
            Token::And => (StructuralOp::And, 2),
            Token::Greater => (StructuralOp::Child, 2),
            Token::Less => (StructuralOp::Parent, 2),
            Token::Descendant => (StructuralOp::Descendant, 2),
            Token::Ancestor => (StructuralOp::Ancestor, 2),
            Token::Sibling => (StructuralOp::Sibling, 2),
            Token::NotChild => (StructuralOp::NotChild, 2),
            Token::NotParent => (StructuralOp::NotParent, 2),
            Token::NotDescendant => (StructuralOp::NotDescendant, 2),
            Token::NotAncestor => (StructuralOp::NotAncestor, 2),
            Token::NotRegex => (StructuralOp::NotSibling, 2),
            Token::UnionChild => (StructuralOp::UnionChild, 2),
            Token::UnionParent => (StructuralOp::UnionParent, 2),
            Token::UnionDescendant => (StructuralOp::UnionDescendant, 2),
            Token::UnionAncestor => (StructuralOp::UnionAncestor, 2),
            Token::UnionSibling => (StructuralOp::UnionSibling, 2),
            _ => return None,
        })
    }

    fn peek_scalar_op(&self) -> Option<(BinaryOp, u8, bool)> {
        Some(match self.peek()?.token {
            Token::Plus => (BinaryOp::Add, 1, false),
            Token::Minus => (BinaryOp::Sub, 1, false),
            Token::Star => (BinaryOp::Mul, 2, false),
            Token::Slash => (BinaryOp::Div, 2, false),
            Token::Percent => (BinaryOp::Mod, 2, false),
            Token::Caret => (BinaryOp::Pow, 3, true),
            _ => return None,
        })
    }

    fn expect(&mut self, expected: Token, message: &str) -> Result<Lexeme, ParseError> {
        let token = self.next_owned(message)?;
        if token.token == expected {
            Ok(token)
        } else {
            Err(ParseError::new(message, token.span))
        }
    }

    fn eat_token(&mut self, expected: Token) -> Option<Lexeme> {
        if self.peek().is_some_and(|token| token.token == expected) {
            self.next().cloned()
        } else {
            None
        }
    }

    fn eat_keyword(&mut self, expected: &str) -> Option<Lexeme> {
        if self.peek().is_some_and(|token| {
            token.token == Token::Ident && token.text.eq_ignore_ascii_case(expected)
        }) {
            self.next().cloned()
        } else {
            None
        }
    }

    fn peek(&self) -> Option<&Lexeme> {
        self.tokens.get(self.position)
    }

    fn next(&mut self) -> Option<&Lexeme> {
        let token = self.tokens.get(self.position);
        if token.is_some() {
            self.position += 1;
        }
        token
    }

    fn next_owned(&mut self, message: &str) -> Result<Lexeme, ParseError> {
        let token = self
            .tokens
            .get(self.position)
            .cloned()
            .ok_or_else(|| self.eof_error(message))?;
        self.position += 1;
        Ok(token)
    }

    fn previous_span(&self) -> SourceSpan {
        self.tokens[self.position - 1].span
    }

    fn eof_error(&self, message: &str) -> ParseError {
        ParseError::new(
            message,
            SourceSpan::new(self.source.len(), self.source.len()),
        )
    }
}

fn parse_field_tokens(tokens: &[Lexeme]) -> Result<FieldExpr, ParseError> {
    let mut output = Vec::new();
    let mut index = 0_usize;
    let mut expects_operand = true;
    while index < tokens.len() {
        let token = &tokens[index];
        let expression_token = match token.token {
            Token::LeftParen => ExprToken::LeftParen(token.span),
            Token::RightParen => ExprToken::RightParen(token.span),
            Token::Or => ExprToken::Or(token.span),
            Token::And => ExprToken::And(token.span),
            Token::Equal => ExprToken::Equal(token.span),
            Token::NotEqual => ExprToken::NotEqual(token.span),
            Token::Regex => ExprToken::Regex(token.span),
            Token::NotRegex => ExprToken::NotRegex(token.span),
            Token::Less => ExprToken::Less(token.span),
            Token::LessEqual => ExprToken::LessEqual(token.span),
            Token::Greater => ExprToken::Greater(token.span),
            Token::GreaterEqual => ExprToken::GreaterEqual(token.span),
            Token::Plus => ExprToken::Plus(token.span),
            Token::Minus if expects_operand => ExprToken::Neg(token.span),
            Token::Minus => ExprToken::Minus(token.span),
            Token::Star => ExprToken::Star(token.span),
            Token::Slash => ExprToken::Slash(token.span),
            Token::Percent => ExprToken::Percent(token.span),
            Token::Caret => ExprToken::Caret(token.span),
            Token::Not => ExprToken::Not(token.span),
            _ => {
                let (operand, consumed) = parse_operand(tokens, index)?;
                index += consumed - 1;
                ExprToken::Operand(operand)
            }
        };
        expects_operand = matches!(
            expression_token,
            ExprToken::Or(_)
                | ExprToken::And(_)
                | ExprToken::Equal(_)
                | ExprToken::NotEqual(_)
                | ExprToken::Regex(_)
                | ExprToken::NotRegex(_)
                | ExprToken::Less(_)
                | ExprToken::LessEqual(_)
                | ExprToken::Greater(_)
                | ExprToken::GreaterEqual(_)
                | ExprToken::Plus(_)
                | ExprToken::Minus(_)
                | ExprToken::Star(_)
                | ExprToken::Slash(_)
                | ExprToken::Percent(_)
                | ExprToken::Caret(_)
                | ExprToken::Not(_)
                | ExprToken::Neg(_)
                | ExprToken::LeftParen(_)
        );
        output.push((token.span.start, expression_token, token.span.end));
        index += 1;
    }
    let span = tokens
        .first()
        .zip(tokens.last())
        .map_or(SourceSpan::default(), |(first, last)| {
            first.span.join(last.span)
        });
    grammar::ExprParser::new()
        .parse(output.into_iter().map(Ok))
        .map_err(|error| ParseError::new(format!("invalid field expression: {error:?}"), span))
}

fn parse_operand(tokens: &[Lexeme], index: usize) -> Result<(FieldExpr, usize), ParseError> {
    let token = &tokens[index];
    let span = token.span;
    let value = match token.token {
        Token::Dot | Token::ResourceDot | Token::SpanDot | Token::InstrumentationDot => {
            let name = tokens
                .get(index + 1)
                .ok_or_else(|| ParseError::new("expected attribute name", span))?;
            if !matches!(name.token, Token::Ident | Token::Quoted | Token::RawString) {
                return Err(ParseError::new("expected attribute name", name.span));
            }
            let mut name_value = if name.token == Token::Ident {
                name.text.clone()
            } else {
                decode_string(name)?
            };
            let mut consumed = 2;
            let mut end_span = name.span;
            if name.token == Token::Ident {
                while tokens
                    .get(index + consumed)
                    .is_some_and(|token| token.token == Token::Dot)
                {
                    let segment = tokens.get(index + consumed + 1).ok_or_else(|| {
                        ParseError::new("expected attribute name segment", end_span)
                    })?;
                    if segment.token != Token::Ident {
                        return Err(ParseError::new(
                            "expected attribute name segment",
                            segment.span,
                        ));
                    }
                    name_value.push('.');
                    name_value.push_str(&segment.text);
                    end_span = segment.span;
                    consumed += 2;
                }
            }
            let scope = match token.token {
                Token::Dot => AttributeScope::Unscoped,
                Token::ResourceDot => AttributeScope::Resource,
                Token::SpanDot => AttributeScope::Span,
                Token::InstrumentationDot => AttributeScope::Instrumentation,
                _ => unreachable!(),
            };
            return Ok((
                Spanned::new(
                    Expr::Attribute(Attribute {
                        scope,
                        name: name_value,
                    }),
                    span.join(end_span),
                ),
                consumed,
            ));
        }
        Token::TraceColon | Token::SpanColon | Token::InstrumentationColon => {
            let name = tokens
                .get(index + 1)
                .ok_or_else(|| ParseError::new("expected intrinsic name", span))?;
            if name.token != Token::Ident {
                return Err(ParseError::new("expected intrinsic name", name.span));
            }
            let intrinsic = scoped_intrinsic(token.token.clone(), &name.text)
                .ok_or_else(|| ParseError::new("unknown scoped intrinsic", span.join(name.span)))?;
            return Ok((
                Spanned::new(Expr::Intrinsic(intrinsic), span.join(name.span)),
                2,
            ));
        }
        Token::Ident => {
            if let Some(intrinsic) = bare_intrinsic(&token.text) {
                Expr::Intrinsic(intrinsic)
            } else {
                Expr::Static(static_from_token(token)?)
            }
        }
        Token::Quoted | Token::RawString | Token::Integer | Token::Float | Token::Duration => {
            Expr::Static(static_from_token(token)?)
        }
        _ => return Err(ParseError::new("expected field operand", span)),
    };
    Ok((Spanned::new(value, span), 1))
}

fn static_from_token(token: &Lexeme) -> Result<StaticValue, ParseError> {
    match token.token {
        Token::Quoted | Token::RawString => decode_string(token).map(StaticValue::String),
        Token::Integer => token
            .text
            .parse()
            .map(StaticValue::Int)
            .map_err(|_| ParseError::new("integer is out of range", token.span)),
        Token::Float => token
            .text
            .parse()
            .map(StaticValue::Float)
            .map_err(|_| ParseError::new("invalid float", token.span)),
        Token::Duration => parse_duration(&token.text)
            .map(StaticValue::Duration)
            .ok_or_else(|| ParseError::new("invalid duration", token.span)),
        Token::Ident => match token.text.as_str() {
            "nil" => Ok(StaticValue::Nil),
            "true" => Ok(StaticValue::Bool(true)),
            "false" => Ok(StaticValue::Bool(false)),
            "unset" => Ok(StaticValue::Status(StatusValue::Unset)),
            "ok" => Ok(StaticValue::Status(StatusValue::Ok)),
            "error" => Ok(StaticValue::Status(StatusValue::Error)),
            "unspecified" => Ok(StaticValue::Kind(KindValue::Unspecified)),
            "internal" => Ok(StaticValue::Kind(KindValue::Internal)),
            "server" => Ok(StaticValue::Kind(KindValue::Server)),
            "client" => Ok(StaticValue::Kind(KindValue::Client)),
            "producer" => Ok(StaticValue::Kind(KindValue::Producer)),
            "consumer" => Ok(StaticValue::Kind(KindValue::Consumer)),
            _ => Err(ParseError::new(
                format!("unknown identifier `{}`", token.text),
                token.span,
            )),
        },
        _ => Err(ParseError::new("expected static value", token.span)),
    }
}

fn parse_duration(source: &str) -> Option<i64> {
    common::time::parse_duration_ns(source).ok()
}

fn scoped_intrinsic(scope: Token, name: &str) -> Option<Intrinsic> {
    match (scope, name) {
        (Token::TraceColon, "id" | "traceID") => Some(Intrinsic::TraceId),
        (Token::TraceColon, "duration" | "traceDuration") => Some(Intrinsic::TraceDuration),
        (Token::TraceColon, "rootName") => Some(Intrinsic::RootName),
        (Token::TraceColon, "rootService") => Some(Intrinsic::RootServiceName),
        (Token::SpanColon, "id" | "spanID") => Some(Intrinsic::SpanId),
        (Token::SpanColon, "parentID") => Some(Intrinsic::ParentId),
        (Token::SpanColon, "name") => Some(Intrinsic::Name),
        (Token::SpanColon, "duration") => Some(Intrinsic::Duration),
        (Token::SpanColon, "status") => Some(Intrinsic::Status),
        (Token::SpanColon, "statusMessage") => Some(Intrinsic::StatusMessage),
        (Token::SpanColon, "kind") => Some(Intrinsic::Kind),
        (Token::SpanColon, "childCount") => Some(Intrinsic::ChildCount),
        (Token::InstrumentationColon, "name") => Some(Intrinsic::InstrumentationName),
        (Token::InstrumentationColon, "version") => Some(Intrinsic::InstrumentationVersion),
        _ => None,
    }
}

fn bare_intrinsic(name: &str) -> Option<Intrinsic> {
    match name {
        "name" => Some(Intrinsic::Name),
        "duration" => Some(Intrinsic::Duration),
        "status" => Some(Intrinsic::Status),
        "statusMessage" => Some(Intrinsic::StatusMessage),
        "kind" => Some(Intrinsic::Kind),
        "rootName" => Some(Intrinsic::RootName),
        "rootServiceName" | "rootService" => Some(Intrinsic::RootServiceName),
        "traceDuration" => Some(Intrinsic::TraceDuration),
        "childCount" => Some(Intrinsic::ChildCount),
        "nestedSetLeft" => Some(Intrinsic::NestedSetLeft),
        "nestedSetRight" => Some(Intrinsic::NestedSetRight),
        "nestedSetParent" => Some(Intrinsic::NestedSetParent),
        _ => None,
    }
}

fn comparison_op(token: &Lexeme) -> Option<BinaryOp> {
    match token.token {
        Token::Equal => Some(BinaryOp::Equal),
        Token::NotEqual => Some(BinaryOp::NotEqual),
        Token::Less => Some(BinaryOp::Less),
        Token::LessEqual => Some(BinaryOp::LessEqual),
        Token::Greater => Some(BinaryOp::Greater),
        Token::GreaterEqual => Some(BinaryOp::GreaterEqual),
        _ => None,
    }
}

fn aggregate_op(name: &str) -> Option<AggregateOp> {
    match name {
        "count" => Some(AggregateOp::Count),
        "min" => Some(AggregateOp::Min),
        "max" => Some(AggregateOp::Max),
        "avg" => Some(AggregateOp::Avg),
        "sum" => Some(AggregateOp::Sum),
        _ => None,
    }
}

fn is_metric_name(name: &str) -> bool {
    matches!(
        name,
        "rate"
            | "count_over_time"
            | "min_over_time"
            | "max_over_time"
            | "avg_over_time"
            | "sum_over_time"
            | "quantile_over_time"
            | "histogram_over_time"
            | "compare"
    )
}
