use super::*;

#[derive(Clone, Copy)]
pub(super) enum RegexKind {
    Plain,
    /// Matches the whole input, as LogQL label matchers do.
    Anchored,
    Pattern,
}

pub(super) const REGEX_KINDS: usize = 3;
pub(super) const REGEX_CACHE_CAPACITY: usize = 256;

thread_local! {
    /// Pipeline stages run per row, so query regexes are compiled once per
    /// worker thread rather than once per row.
    static REGEX_CACHE: RefCell<[HashMap<String, Regex>; REGEX_KINDS]> =
        RefCell::new(Default::default());
}

/// `f` must not itself call `with_regex`.
pub(super) fn with_regex<T>(
    kind: RegexKind,
    source: &str,
    f: impl FnOnce(&Regex) -> T,
) -> Result<T> {
    REGEX_CACHE.with(|caches| {
        if let Some(regex) = caches.borrow()[kind as usize].get(source) {
            return Ok(f(regex));
        }
        let regex = match kind {
            RegexKind::Plain => Regex::new(source)?,
            RegexKind::Anchored => Regex::new(&format!("^(?:{source})$"))?,
            RegexKind::Pattern => pattern_regex(source)?,
        };
        let result = f(&regex);
        let mut caches = caches.borrow_mut();
        let cache = &mut caches[kind as usize];
        if cache.len() >= REGEX_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(source.to_owned(), regex);
        Ok(result)
    })
}

thread_local! {
    static MATCH_TERMS_CACHE: RefCell<HashMap<String, Rc<[String]>>> =
        RefCell::new(HashMap::new());
}

pub(super) fn match_terms(query: &str) -> Rc<[String]> {
    MATCH_TERMS_CACHE.with(|cache| {
        if let Some(terms) = cache.borrow().get(query) {
            return Rc::clone(terms);
        }
        let terms: Rc<[String]> = query_terms(&DEFAULT_ANALYZER, query).into();
        let mut cache = cache.borrow_mut();
        if cache.len() >= REGEX_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(query.to_owned(), Rc::clone(&terms));
        terms
    })
}

/// The anchored regex behind the `|>` / `!>` line filters, where every
/// `<capture>` matches lazily.
pub(super) fn pattern_regex(pattern: &str) -> Result<Regex> {
    let mut source = String::new();
    let mut rest = pattern;
    while let Some(start) = rest.find('<') {
        let Some(relative_end) = rest[start + 1..].find('>') else {
            break;
        };
        let end = start + relative_end + 1;
        source.push_str(&regex::escape(&rest[..start]));
        source.push_str(".*?");
        rest = &rest[end + 1..];
    }
    source.push_str(&regex::escape(rest));
    Regex::new(&format!("(?s)^{source}$")).map_err(Into::into)
}
