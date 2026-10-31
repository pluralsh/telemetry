//! Crate-wide utility helpers.
use bytemuck::NoUninit;
use bytes::Bytes;

/// Owner wrapper for a `Vec<T>` exposed as its raw byte representation, so
/// we can hand ownership to `Bytes::from_owner` while preserving the natural
/// alignment of the underlying allocation.
struct AlignedBuf<T: NoUninit>(Vec<T>);

impl<T: NoUninit> AsRef<[u8]> for AlignedBuf<T> {
    fn as_ref(&self) -> &[u8] {
        bytemuck::cast_slice(&self.0)
    }
}

/// Turn a `Vec<T>` into a `Bytes` whose pointer is guaranteed to have at least
/// `align_of::<T>()` alignment (the Vec's natural allocator alignment).
pub(crate) fn aligned_bytes_from_vec<T: NoUninit + Send + 'static>(vec: Vec<T>) -> Bytes {
    Bytes::from_owner(AlignedBuf(vec))
}

/// Matches `text` against a glob `pattern`: `*` matches any run of
/// characters, `?` any one character, `[abc]` / `[a-z]` / `[!a-z]` a
/// character class, and `\` escapes the next character.
pub(crate) fn glob_match(pattern: &str, text: &str, case_insensitive: bool) -> bool {
    let fold = |s: &str| -> Vec<char> {
        if case_insensitive {
            s.chars().flat_map(char::to_lowercase).collect()
        } else {
            s.chars().collect()
        }
    };
    let pattern = fold(pattern);
    let text = fold(text);

    // Iterative matcher with single-star backtracking.
    let (mut p, mut t) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                '*' => {
                    star = Some((p, t));
                    p += 1;
                    continue;
                }
                '?' => {
                    p += 1;
                    t += 1;
                    continue;
                }
                '[' => {
                    if let Some((matched, next)) = match_class(&pattern, p, text[t]) {
                        if matched {
                            p = next;
                            t += 1;
                            continue;
                        }
                    } else if text[t] == '[' {
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
                '\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == text[t] {
                        p += 2;
                        t += 1;
                        continue;
                    }
                }
                c => {
                    if c == text[t] {
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
            }
        }
        match star {
            Some((star_p, star_t)) => {
                p = star_p + 1;
                t = star_t + 1;
                star = Some((star_p, star_t + 1));
            }
            None => return false,
        }
    }
    pattern[p..].iter().all(|&c| c == '*')
}

/// Matches `c` against the class starting at `pattern[start] == '['`.
/// Returns `(matched, index after the class)`, or `None` if unterminated.
fn match_class(pattern: &[char], start: usize, c: char) -> Option<(bool, usize)> {
    let mut i = start + 1;
    let negated = matches!(pattern.get(i), Some('!') | Some('^'));
    if negated {
        i += 1;
    }
    let mut matched = false;
    let mut first = true;
    while i < pattern.len() {
        let lo = pattern[i];
        if lo == ']' && !first {
            return Some((matched != negated, i + 1));
        }
        first = false;
        if i + 2 < pattern.len() && pattern[i + 1] == '-' && pattern[i + 2] != ']' {
            if lo <= c && c <= pattern[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if lo == c {
                matched = true;
            }
            i += 1;
        }
    }
    None
}

/// The literal prefix of a glob pattern (everything before the first
/// wildcard), used to bound index scans.
pub(crate) fn glob_literal_prefix(pattern: &str) -> String {
    let mut prefix = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '*' | '?' | '[' => break,
            '\\' => match chars.next() {
                Some(next) => prefix.push(next),
                None => break,
            },
            c => prefix.push(c),
        }
    }
    prefix
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_match_globs() {
        assert!(glob_match("foo*", "foobar", false));
        assert!(glob_match("*bar", "foobar", false));
        assert!(glob_match("f?o*r", "foobar", false));
        assert!(glob_match("*", "", false));
        assert!(!glob_match("foo", "foobar", false));
        assert!(glob_match("[a-c]at", "bat", false));
        assert!(!glob_match("[!a-c]at", "bat", false));
        assert!(glob_match("*.rs", "src/main.rs", false));
        assert!(glob_match("FOO*", "foobar", true));
        assert!(!glob_match("FOO*", "foobar", false));
        assert!(glob_match("a\\*b", "a*b", false));
        assert!(!glob_match("a\\*b", "axb", false));
    }

    #[test]
    fn should_extract_glob_literal_prefix() {
        assert_eq!(glob_literal_prefix("foo*bar"), "foo");
        assert_eq!(glob_literal_prefix("*foo"), "");
        assert_eq!(glob_literal_prefix("a\\*b?"), "a*b");
        assert_eq!(glob_literal_prefix("plain"), "plain");
    }
}
