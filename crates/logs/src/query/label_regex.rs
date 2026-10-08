//! Loki evaluates label filter regexes (`| label=~"..."`) through
//! `log.parseRegexpFilter`, which rewrites simple patterns into literal
//! filters. The rewrite drops anchoring: `foo.*`, `.*foo` and `.*foo.*` all
//! become substring checks, `""` matches every value and `.+` any non-empty
//! one. Whether a pattern is rewritten depends on Go's regexp parser, which
//! factors common prefixes out of alternations (`u37|u74` becomes `u(?:37|74)`,
//! a substring check) and merges single characters into classes (`a|b`
//! becomes `[a-b]`, left as a regex), so that factoring is reproduced here.

use regex_syntax::ast::{self, Ast, Flag, FlagsItemKind, GroupKind, RepetitionKind};

use super::contains;

/// The `bool` of `Equal` and `Contains` is case folding; folded text is
/// stored lowercased.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum LabelRegexFilter {
    True,
    Exists,
    Equal(String, bool),
    Contains(String, bool),
    Or(Box<LabelRegexFilter>, Box<LabelRegexFilter>),
}

impl LabelRegexFilter {
    pub(super) fn matches(&self, value: &str) -> bool {
        match self {
            Self::True => true,
            Self::Exists => !value.is_empty(),
            Self::Equal(expected, false) => value == expected,
            Self::Equal(expected, true) => value.to_lowercase() == *expected,
            Self::Contains(expected, false) => contains(value, expected),
            Self::Contains(expected, true) => contains(&value.to_lowercase(), expected),
            Self::Or(lhs, rhs) => lhs.matches(value) || rhs.matches(value),
        }
    }

    fn equal(text: String, fold: bool) -> Self {
        Self::Equal(folded(text, fold), fold)
    }

    fn contains(text: String, fold: bool) -> Self {
        Self::Contains(folded(text, fold), fold)
    }

    fn or(current: Option<Self>, next: Self) -> Self {
        match current {
            Some(current) => Self::Or(Box::new(current), Box::new(next)),
            None => next,
        }
    }
}

fn folded(text: String, fold: bool) -> String {
    if fold { text.to_lowercase() } else { text }
}

/// `None` when Loki would fall back to an anchored regex.
pub(super) fn simplify_label_regex(source: &str) -> Option<LabelRegexFilter> {
    let ast = ast::parse::Parser::new().parse(source).ok()?;
    let mut flags = Flags::default();
    simplify(&Node::from_ast(&ast, &mut flags)?)
}

#[derive(Clone, Copy, Default)]
struct Flags {
    fold: bool,
    dot_newline: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum Node {
    Empty,
    Literal {
        text: String,
        fold: bool,
    },
    AnyStar,
    AnyPlus,
    /// A single-character matcher Go merges into a class within alternations.
    Class,
    Capture(Box<Node>),
    Concat(Vec<Node>),
    Alternate(Vec<Node>),
    Other,
}

impl Node {
    fn from_ast(ast: &Ast, flags: &mut Flags) -> Option<Self> {
        Some(match ast {
            Ast::Empty(_) => Self::Empty,
            Ast::Flags(set) => {
                apply_flags(&set.flags, flags)?;
                Self::Empty
            }
            Ast::Literal(literal) => Self::Literal {
                text: literal.c.to_string(),
                fold: flags.fold,
            },
            Ast::Dot(_) | Ast::ClassUnicode(_) | Ast::ClassPerl(_) | Ast::ClassBracketed(_) => {
                Self::Class
            }
            Ast::Assertion(_) => Self::Other,
            Ast::Repetition(repetition) => {
                let any = matches!(*repetition.ast, Ast::Dot(_)) && !flags.dot_newline;
                match (&repetition.op.kind, any) {
                    (RepetitionKind::ZeroOrMore, true) => Self::AnyStar,
                    (RepetitionKind::OneOrMore, true) => Self::AnyPlus,
                    (RepetitionKind::Range(ast::RepetitionRange::AtLeast(0)), true) => {
                        Self::AnyStar
                    }
                    (RepetitionKind::Range(ast::RepetitionRange::AtLeast(1)), true) => {
                        Self::AnyPlus
                    }
                    _ => Self::Other,
                }
            }
            Ast::Group(group) => {
                let saved = *flags;
                let node = match &group.kind {
                    GroupKind::NonCapturing(group_flags) => {
                        apply_flags(group_flags, flags)?;
                        Self::from_ast(&group.ast, flags)?
                    }
                    _ => Self::Capture(Box::new(Self::from_ast(&group.ast, flags)?)),
                };
                *flags = saved;
                node
            }
            Ast::Alternation(alternation) => {
                let branches = alternation
                    .asts
                    .iter()
                    .map(|branch| Self::from_ast(branch, flags))
                    .collect::<Option<Vec<_>>>()?;
                alternate(branches)
            }
            Ast::Concat(sequence) => {
                let mut items = Vec::new();
                let mut raw_literal = false;
                for item in &sequence.asts {
                    let node = Self::from_ast(item, flags)?;
                    let literal = matches!(item, Ast::Literal(_));
                    match (node, items.last_mut()) {
                        (
                            Self::Literal { text, fold },
                            Some(Self::Literal {
                                text: previous,
                                fold: previous_fold,
                            }),
                        ) if literal && raw_literal && fold == *previous_fold => {
                            previous.push_str(&text);
                        }
                        (Self::Empty, _) => {}
                        (Self::Concat(inner), _) => items.extend(inner),
                        (node, _) => items.push(node),
                    }
                    raw_literal = literal;
                }
                concat(items)
            }
        })
    }

    fn leading_string(&self) -> Option<(&str, bool)> {
        let first = match self {
            Self::Concat(items) => items.first()?,
            node => node,
        };
        match first {
            Self::Literal { text, fold } => Some((text, *fold)),
            _ => None,
        }
    }

    fn remove_leading_chars(self, count: usize) -> Self {
        let strip = |text: String| text.chars().skip(count).collect::<String>();
        match self {
            Self::Literal { text, fold } => {
                let text = strip(text);
                if text.is_empty() {
                    Self::Empty
                } else {
                    Self::Literal { text, fold }
                }
            }
            Self::Concat(mut items) => {
                if let Some(Self::Literal { text, .. }) = items.first_mut() {
                    *text = strip(std::mem::take(text));
                    if text.is_empty() {
                        items.remove(0);
                    }
                }
                concat(items)
            }
            node => node,
        }
    }

    fn is_char_class(&self) -> bool {
        match self {
            Self::Class => true,
            Self::Literal { text, .. } => text.chars().count() == 1,
            _ => false,
        }
    }
}

fn apply_flags(group_flags: &ast::Flags, flags: &mut Flags) -> Option<()> {
    let mut enable = true;
    for item in &group_flags.items {
        match item.kind {
            FlagsItemKind::Negation => enable = false,
            FlagsItemKind::Flag(Flag::CaseInsensitive) => flags.fold = enable,
            FlagsItemKind::Flag(Flag::DotMatchesNewLine) => flags.dot_newline = enable,
            FlagsItemKind::Flag(Flag::MultiLine | Flag::SwapGreed) => {}
            FlagsItemKind::Flag(_) => return None,
        }
    }
    Some(())
}

fn concat(items: Vec<Node>) -> Node {
    let mut flattened = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Node::Concat(inner) => flattened.extend(inner),
            item => flattened.push(item),
        }
    }
    match flattened.len() {
        0 => Node::Empty,
        1 => flattened.pop().expect("one item"),
        _ => Node::Concat(flattened),
    }
}

fn alternate(branches: Vec<Node>) -> Node {
    let mut flattened = Vec::with_capacity(branches.len());
    for branch in branches {
        match branch {
            Node::Alternate(inner) => flattened.extend(inner),
            branch => flattened.push(branch),
        }
    }
    let mut factored = factor(flattened);
    if factored.len() == 1 {
        factored.pop().expect("one branch")
    } else {
        Node::Alternate(factored)
    }
}

/// Go's `parser.factor`: common literal prefixes of adjacent branches are
/// pulled out, then runs of single characters become one class.
fn factor(branches: Vec<Node>) -> Vec<Node> {
    let mut prefixed = Vec::with_capacity(branches.len());
    let mut run: Vec<Node> = Vec::new();
    let mut prefix: Option<(String, bool)> = None;
    let flush = |run: &mut Vec<Node>, prefix: &Option<(String, bool)>, out: &mut Vec<Node>| match (
        run.len(),
        prefix,
    ) {
        (0, _) => {}
        (1, _) | (_, None) => out.append(run),
        (_, Some((text, fold))) => {
            let count = text.chars().count();
            let suffixes = run
                .drain(..)
                .map(|branch| branch.remove_leading_chars(count))
                .collect();
            out.push(concat(vec![
                Node::Literal {
                    text: text.clone(),
                    fold: *fold,
                },
                alternate(suffixes),
            ]));
        }
    };
    for branch in branches {
        let leading = branch
            .leading_string()
            .map(|(text, fold)| (text.to_owned(), fold));
        let common = match (&prefix, &leading) {
            (Some((current, fold)), Some((next, next_fold))) if fold == next_fold => {
                let shared = current
                    .chars()
                    .zip(next.chars())
                    .take_while(|(lhs, rhs)| lhs == rhs)
                    .map(|(character, _)| character)
                    .collect::<String>();
                (!shared.is_empty()).then_some((shared, *fold))
            }
            _ => None,
        };
        if common.is_some() && !run.is_empty() {
            prefix = common;
            run.push(branch);
            continue;
        }
        flush(&mut run, &prefix, &mut prefixed);
        prefix = leading;
        run.push(branch);
    }
    flush(&mut run, &prefix, &mut prefixed);

    let mut out: Vec<Node> = Vec::with_capacity(prefixed.len());
    let mut class_run = 0;
    for branch in prefixed {
        let class = branch.is_char_class();
        if class && class_run > 0 {
            *out.last_mut().expect("class run") = Node::Class;
        } else if branch == Node::Empty && out.last() == Some(&Node::Empty) {
            continue;
        } else {
            out.push(branch);
        }
        class_run = if class { class_run + 1 } else { 0 };
    }
    out
}

fn uncapture(node: &Node) -> &Node {
    match node {
        Node::Capture(inner) => inner,
        node => node,
    }
}

/// Loki's `RegexSimplifier.Simplify` with `isLabel` set.
fn simplify(node: &Node) -> Option<LabelRegexFilter> {
    match node {
        Node::Alternate(branches) => {
            let mut filter = None;
            for branch in branches {
                filter = Some(LabelRegexFilter::or(filter, simplify(uncapture(branch))?));
            }
            filter
        }
        Node::Concat(items) => simplify_concat(items, String::new(), false),
        Node::Capture(inner) => simplify(inner),
        Node::Literal { text, fold } => Some(LabelRegexFilter::equal(text.clone(), *fold)),
        Node::AnyStar | Node::Empty => Some(LabelRegexFilter::True),
        Node::AnyPlus => Some(LabelRegexFilter::Exists),
        Node::Class | Node::Other => None,
    }
}

fn simplify_concat(items: &[Node], mut base: String, has_base: bool) -> Option<LabelRegexFilter> {
    let items = items
        .iter()
        .map(uncapture)
        .filter(|item| **item != Node::Empty)
        .collect::<Vec<_>>();
    if items.len() > 3 {
        return None;
    }
    let mut has_base = has_base;
    let mut literals = 0;
    let mut base_fold = false;
    let mut current = None;
    for item in items {
        match item {
            Node::Literal { text, fold } => {
                if literals != 0 {
                    return None;
                }
                literals += 1;
                base.push_str(text);
                has_base = true;
                base_fold = *fold;
            }
            Node::Alternate(branches) if has_base => {
                current = Some(simplify_concat_alternate(
                    branches, &base, base_fold, current,
                )?);
            }
            Node::AnyStar => {}
            _ => return None,
        }
    }
    current.or_else(|| has_base.then(|| LabelRegexFilter::contains(base, base_fold)))
}

fn simplify_concat_alternate(
    branches: &[Node],
    base: &str,
    base_fold: bool,
    mut current: Option<LabelRegexFilter>,
) -> Option<LabelRegexFilter> {
    for branch in branches {
        let filter = match branch {
            Node::Literal { fold: true, .. } if !base_fold => return None,
            Node::Empty | Node::AnyStar => LabelRegexFilter::contains(base.to_owned(), base_fold),
            Node::Literal { text, .. } => {
                LabelRegexFilter::contains(format!("{base}{text}"), base_fold)
            }
            Node::Concat(items) => simplify_concat(items, base.to_owned(), true)?,
            _ => return None,
        };
        current = Some(LabelRegexFilter::or(current, filter));
    }
    current
}

#[cfg(test)]
mod tests {
    use super::LabelRegexFilter::{self, *};
    use super::simplify_label_regex;

    fn contains(text: &str) -> LabelRegexFilter {
        Contains(text.into(), false)
    }

    fn equal(text: &str) -> LabelRegexFilter {
        Equal(text.into(), false)
    }

    fn or(lhs: LabelRegexFilter, rhs: LabelRegexFilter) -> LabelRegexFilter {
        Or(Box::new(lhs), Box::new(rhs))
    }

    #[test]
    fn matches_lokis_regex_simplifier() {
        let cases = [
            ("foo", Some(equal("foo"))),
            ("foo.*", Some(contains("foo"))),
            (".*foo", Some(contains("foo"))),
            (".*foo.*", Some(contains("foo"))),
            (".*", Some(True)),
            ("", Some(True)),
            (".+", Some(Exists)),
            ("error|warn", Some(or(equal("error"), equal("warn")))),
            (
                ".*compaction|queue.*",
                Some(or(contains("compaction"), contains("queue"))),
            ),
            ("u37|u74", Some(or(contains("u37"), contains("u74")))),
            (
                "GET|POST|PUT",
                Some(or(equal("GET"), or(contains("POST"), contains("PUT")))),
            ),
            ("foo(bar|baz)", None),
            (
                "foo(?:bar|qux)",
                Some(or(contains("foobar"), contains("fooqux"))),
            ),
            ("a|b", None),
            ("ba|bb", None),
            ("foo.*bar", None),
            (r"\d+", None),
            ("(?i)foo", Some(Equal("foo".into(), true))),
            ("(?i)FoO", Some(Equal("foo".into(), true))),
            ("(?i).*FOO.*", Some(Contains("foo".into(), true))),
            ("(?s).*", None),
            ("^foo$", None),
        ];
        for (source, expected) in cases {
            let actual = simplify_label_regex(source);
            assert_eq!(actual.is_some(), expected.is_some(), "{source}: {actual:?}");
            let probes = [
                "", "foo", "xfoox", "GET", "xPOSTx", "u370", "a", "queue x", "FOO",
            ];
            if let (Some(actual), Some(expected)) = (actual, expected) {
                for probe in probes {
                    assert_eq!(
                        actual.matches(probe),
                        expected.matches(probe),
                        "{source} on {probe:?}"
                    );
                }
            }
        }
    }
}
