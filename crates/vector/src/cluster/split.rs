//! Splitting a document's text into passages that fit an embedding model.
//!
//! The text is cut at the coarsest boundaries that give pieces of at most
//! `max_chars` characters: paragraphs, then lines, sentences (ICU4X sentence
//! segmentation), words, and finally a hard cut. Consecutive pieces are then
//! packed into passages of at most `max_chars`, each starting with up to
//! `overlap_chars` of the previous passage's trailing pieces.

use icu_segmenter::options::SentenceBreakInvariantOptions;
use icu_segmenter::{SentenceSegmenter, SentenceSegmenterBorrowed};

thread_local! {
    static SENTENCES: SentenceSegmenterBorrowed<'static> =
        SentenceSegmenter::new(SentenceBreakInvariantOptions::default());
}

/// Boundaries tried in order, coarsest first.
#[derive(Debug, Clone, Copy)]
enum Level {
    Paragraph,
    Line,
    Sentence,
    Word,
    Char,
}

impl Level {
    fn finer(self) -> Self {
        match self {
            Level::Paragraph => Level::Line,
            Level::Line => Level::Sentence,
            Level::Sentence => Level::Word,
            Level::Word | Level::Char => Level::Char,
        }
    }
}

fn chars(text: &str) -> usize {
    text.chars().count()
}

/// The passages of `text`, each at most `max_chars` characters and trimmed
/// of surrounding whitespace. Empty for blank text.
pub(crate) fn split(text: &str, max_chars: usize, overlap_chars: usize) -> Vec<String> {
    let max_chars = max_chars.max(1);
    let overlap_chars = overlap_chars.min(max_chars - 1);
    let mut pieces = Vec::new();
    collect(text, Level::Paragraph, max_chars, &mut pieces);
    pack(&pieces, max_chars, overlap_chars)
}

/// Appends `text`'s pieces of at most `max_chars`, cut at `level` and
/// finer boundaries. The pieces concatenate back to `text`.
fn collect<'a>(text: &'a str, level: Level, max_chars: usize, out: &mut Vec<&'a str>) {
    if chars(text) <= max_chars {
        if !text.is_empty() {
            out.push(text);
        }
        return;
    }
    for piece in cut(text, level, max_chars) {
        if matches!(level, Level::Char) {
            out.push(piece);
        } else {
            collect(piece, level.finer(), max_chars, out);
        }
    }
}

/// `text` cut after each boundary of `level`, keeping separators with the
/// piece before them.
fn cut(text: &str, level: Level, max_chars: usize) -> Vec<&str> {
    let ends: Vec<usize> = match level {
        Level::Paragraph => separator_ends(text, "\n\n"),
        Level::Line => separator_ends(text, "\n"),
        Level::Sentence => SENTENCES.with(|s| s.segment_str(text).skip(1).collect()),
        Level::Word => text
            .char_indices()
            .filter(|(_, c)| c.is_whitespace())
            .map(|(i, c)| i + c.len_utf8())
            .collect(),
        Level::Char => text
            .char_indices()
            .map(|(i, _)| i)
            .skip(max_chars)
            .step_by(max_chars)
            .collect(),
    };
    let mut pieces = Vec::with_capacity(ends.len() + 1);
    let mut start = 0;
    for end in ends {
        if end > start && end < text.len() {
            pieces.push(&text[start..end]);
            start = end;
        }
    }
    pieces.push(&text[start..]);
    pieces
}

fn separator_ends(text: &str, separator: &str) -> Vec<usize> {
    text.match_indices(separator)
        .map(|(i, s)| i + s.len())
        .collect()
}

/// Packs consecutive `pieces` into passages of at most `max_chars`.
fn pack(pieces: &[&str], max_chars: usize, overlap_chars: usize) -> Vec<String> {
    let mut passages = Vec::new();
    // The pieces of the passage being built, and its length. Between
    // pieces it always ends with one not yet emitted.
    let mut current: Vec<&str> = Vec::new();
    let mut length = 0;
    for piece in pieces {
        let size = chars(piece);
        if length + size > max_chars && !current.is_empty() {
            emit(&current, &mut passages);
            let mut kept = 0;
            let mut carried = 0;
            for piece in current.iter().rev() {
                let size = chars(piece);
                if carried + size > overlap_chars {
                    break;
                }
                carried += size;
                kept += 1;
            }
            current.drain(..current.len() - kept);
            length = carried;
        }
        while length + size > max_chars && !current.is_empty() {
            length -= chars(current.remove(0));
        }
        current.push(piece);
        length += size;
    }
    if !current.is_empty() {
        emit(&current, &mut passages);
    }
    passages
}

fn emit(pieces: &[&str], passages: &mut Vec<String>) {
    let passage = pieces.concat();
    let passage = passage.trim();
    if !passage.is_empty() {
        passages.push(passage.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_keep_short_text_whole() {
        assert_eq!(split("  one passage \n", 100, 0), vec!["one passage"]);
        assert!(split(" \n\n ", 100, 0).is_empty());
    }

    #[test]
    fn should_split_at_the_coarsest_boundary_that_fits() {
        // given
        let text = "First paragraph here.\n\nSecond one. It has two sentences.";

        // then: paragraphs fit whole
        assert_eq!(
            split(text, 40, 0),
            vec!["First paragraph here.", "Second one. It has two sentences."]
        );
        // then: the long paragraph splits into sentences
        assert_eq!(
            split(text, 25, 0),
            vec![
                "First paragraph here.",
                "Second one.",
                "It has two sentences."
            ]
        );
        // then: a long sentence splits into words, then characters
        assert_eq!(split("abc defghij", 4, 0), vec!["abc", "defg", "hij"]);
    }

    #[test]
    fn should_pack_pieces_up_to_the_limit() {
        let text = "a.\nb.\nc.\nd.\n";
        assert_eq!(split(text, 6, 0), vec!["a.\nb.", "c.\nd."]);
    }

    #[test]
    fn should_overlap_passages() {
        // given
        let text = "one two three four five";

        // when
        let passages = split(text, 10, 4);

        // then: each passage repeats the trailing word of the one before
        assert_eq!(passages, vec!["one two", "two three", "four five"]);
        for passage in &passages {
            assert!(chars(passage) <= 10);
        }
    }

    #[test]
    fn should_bound_every_passage_and_lose_no_text() {
        let text = "Ünïcödé wörds, and a looooooooooooooooong one.\n\nMore.";
        for max in 1..20 {
            let passages = split(text, max, 0);
            assert!(
                passages.iter().all(|p| chars(p) <= max),
                "{max}: {passages:?}"
            );
            let joined: String = passages.concat();
            let expected: String = text.chars().filter(|c| !c.is_whitespace()).collect();
            let actual: String = joined.chars().filter(|c| !c.is_whitespace()).collect();
            assert_eq!(actual, expected, "{max}");
        }
    }
}
