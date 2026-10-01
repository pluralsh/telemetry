// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::borrow::Cow;

use linkify::LinkFinder;
use unicode_segmentation::UnicodeSegmentation;

/// Produces normalized terms for Logs' full-text index and match queries.
///
/// Implementations must be deterministic: changing analyzer behavior for an
/// existing database requires rebuilding its full-text index.
pub trait Analyzer: std::fmt::Debug + Send + Sync {
    /// Visits terms in source order without requiring an intermediate
    /// collection. Terms only need to remain valid for the visitor call.
    fn for_each_term(&self, value: &str, visitor: &mut dyn FnMut(&str));
}

/// Logs' default analyzer for unstructured log messages.
///
/// URLs and email addresses are preserved as complete terms. All remaining
/// text is split according to Unicode Standard Annex #29 word boundaries, and
/// every resulting term is lowercased.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogAnalyzer;

impl Analyzer for LogAnalyzer {
    fn for_each_term(&self, value: &str, visitor: &mut dyn FnMut(&str)) {
        for span in LinkFinder::new().spans(value) {
            if span.kind().is_some() {
                visit_normalized(span.as_str(), visitor);
            } else {
                for term in span.as_str().unicode_words() {
                    visit_normalized(term, visitor);
                }
            }
        }
    }
}

/// Equivalent to `str::to_lowercase`, borrowing when the term is already
/// lowercase ASCII.
fn normalize(term: &str) -> Cow<'_, str> {
    if term.is_ascii() && !term.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Borrowed(term)
    } else {
        Cow::Owned(term.to_lowercase())
    }
}

fn visit_normalized(term: &str, visitor: &mut dyn FnMut(&str)) {
    visitor(&normalize(term));
}

pub(crate) static DEFAULT_ANALYZER: LogAnalyzer = LogAnalyzer;
