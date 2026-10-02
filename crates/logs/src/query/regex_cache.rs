use super::*;

#[derive(Clone, Copy)]
pub(super) enum RegexKind {
    Plain,
    /// Matches the whole input, as LogQL label matchers do.
    Anchored,
}

pub(super) const REGEX_KINDS: usize = 2;
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
    static LABEL_REGEX_CACHE: RefCell<HashMap<String, Option<Rc<LabelRegexFilter>>>> =
        RefCell::new(HashMap::new());
}

pub(super) fn label_regex_filter(source: &str) -> Option<Rc<LabelRegexFilter>> {
    LABEL_REGEX_CACHE.with(|cache| {
        if let Some(filter) = cache.borrow().get(source) {
            return filter.clone();
        }
        let filter = simplify_label_regex(source).map(Rc::new);
        let mut cache = cache.borrow_mut();
        if cache.len() >= REGEX_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(source.to_owned(), filter.clone());
        filter
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
