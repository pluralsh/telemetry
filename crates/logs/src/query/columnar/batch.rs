//! A batch of one stream's rows, and the per-expression state its pipeline
//! stages change.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::ops::Range;
use std::sync::Arc;

use super::super::parallel::BATCH_ROWS;
use super::super::pipeline::{LogfmtValue, StageRow, logfmt_pairs, lookup, set_error};
use super::super::template::Context;
use super::super::{ERROR_LABEL, LabelMap, SCORE_METADATA_FIELD, stream_hash};
use crate::Labels;

/// Where a stored row stands in Loki's merge order among rows sharing its
/// timestamp and stream: by database, segment, object, then row.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Tie {
    pub(crate) database: u32,
    pub(crate) segment: u32,
    pub(crate) object: u64,
    pub(crate) row: u64,
}

/// Rows of one stored stream, in read order, with lines and structured
/// metadata values in one buffer.
pub(crate) struct ColumnBatch {
    pub(super) labels: LabelMap,
    /// Loki's hash of the stream's labels.
    pub(super) stream: u64,
    pub(super) timestamps: Vec<i64>,
    text: String,
    /// Per row, its line's span in `text`; empty for a lineless batch.
    lines: Vec<(u32, u32)>,
    /// Per row, its metadata's span in `fields`.
    field_spans: Vec<(u32, u32)>,
    /// A metadata field: its name's index in `names`, and its value in `text`.
    fields: Vec<(u32, u32, u32)>,
    names: Vec<Arc<str>>,
    /// For a lineless batch, per row the stored rows it stands for and their
    /// line bytes.
    pub(super) weights: Option<Vec<(u32, u64)>>,
    /// The first row's tie; row `i` follows it by `i`.
    tie: Tie,
}

impl ColumnBatch {
    pub(super) fn new(labels: LabelMap, stream: u64, lineless: bool, tie: Tie) -> Self {
        Self {
            labels,
            stream,
            timestamps: Vec::new(),
            text: String::new(),
            lines: Vec::new(),
            field_spans: Vec::new(),
            fields: Vec::new(),
            names: Vec::new(),
            weights: lineless.then(Vec::new),
            tie,
        }
    }

    /// An empty batch of a stored stream's rows.
    pub(crate) fn of_stream(labels: &Labels, tie: Tie) -> Self {
        let map: BTreeMap<String, String> = labels
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect();
        let stream = stream_hash(&map);
        Self::new(Arc::new(map), stream, false, tie)
    }

    pub(crate) fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub(crate) fn timestamps(&self) -> &[i64] {
        &self.timestamps
    }

    pub(super) fn tie(&self, row: usize) -> Tie {
        Tie {
            row: self.tie.row + row as u64,
            ..self.tie
        }
    }

    /// Appends a row; `metadata` need not be in name order.
    pub(crate) fn push<'m>(
        &mut self,
        timestamp_ns: i64,
        line: &str,
        metadata: impl IntoIterator<Item = (&'m str, &'m str)>,
        weight: Option<(u32, u64)>,
    ) {
        self.timestamps.push(timestamp_ns);
        match &mut self.weights {
            Some(weights) => weights.push(weight.unwrap_or_default()),
            None => {
                let start = offset(self.text.len());
                self.text.push_str(line);
                self.lines.push((start, offset(self.text.len())));
            }
        }
        let first = offset(self.fields.len());
        for (name, value) in metadata {
            let index = match self.names.iter().position(|known| &**known == name) {
                Some(index) => index,
                None => {
                    self.names.push(Arc::from(name));
                    self.names.len() - 1
                }
            };
            let start = offset(self.text.len());
            self.text.push_str(value);
            self.fields
                .push((offset(index), start, offset(self.text.len())));
        }
        self.field_spans.push((first, offset(self.fields.len())));
    }

    /// Puts rows in timestamp order, keeping read order among equal ones.
    pub(crate) fn sort_by_timestamp(&mut self) {
        if self.timestamps.is_sorted() {
            return;
        }
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_by_key(|&row| self.timestamps[row]);
        permute(&mut self.timestamps, &order);
        permute(&mut self.lines, &order);
        permute(&mut self.field_spans, &order);
        if let Some(weights) = &mut self.weights {
            permute(weights, &order);
        }
    }

    pub(super) fn line(&self, row: usize) -> &str {
        self.lines
            .get(row)
            .map_or("", |&(start, end)| self.span(start, end))
    }

    fn metadata(&self, row: usize) -> &[(u32, u32, u32)] {
        let (start, end) = self.field_spans[row];
        &self.fields[start as usize..end as usize]
    }

    /// Identifies a stored row among those sharing its timestamp and stream:
    /// its stored line and structured metadata, as Loki's merge compares
    /// them.
    pub(super) fn identity(&self, row: usize) -> u64 {
        let mut fields = self
            .metadata(row)
            .iter()
            .map(|&(name, start, end)| (&*self.names[name as usize], self.span(start, end)))
            .collect::<Vec<_>>();
        fields.sort_unstable();
        let mut hasher = foldhash::quality::FixedState::default().build_hasher();
        hasher.write(self.line(row).as_bytes());
        hasher.write_u8(0xff);
        for (name, value) in fields {
            write_pair(&mut hasher, name, value);
        }
        hasher.finish()
    }

    fn span(&self, start: u32, end: u32) -> &str {
        &self.text[start as usize..end as usize]
    }
}

/// Rows `rows` of a shared batch: what a read releases at once.
pub(crate) struct BatchSlice {
    pub(super) batch: Arc<ColumnBatch>,
    pub(super) rows: Range<usize>,
}

impl BatchSlice {
    pub(crate) fn new(batch: Arc<ColumnBatch>, rows: Range<usize>) -> Self {
        Self { batch, rows }
    }

    pub(super) fn whole(batch: ColumnBatch) -> Self {
        let rows = 0..batch.len();
        Self::new(Arc::new(batch), rows)
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Groups rows as they are read into per-stream batches: consecutive rows of
/// one stream share a batch, and each stream's label map is built once.
pub(crate) struct StreamBatches {
    lineless: bool,
    /// Keyed by `Arc` address; holding the `Arc` keeps the address from being
    /// reused by another stream's labels.
    streams: foldhash::HashMap<usize, (Arc<Labels>, LabelMap, u64)>,
    current: Option<(usize, ColumnBatch)>,
    ready: Vec<BatchSlice>,
    /// Rows batched so far, which order rows as they were read.
    rows: u64,
}

impl StreamBatches {
    /// Batches of lines, or with `lineless`, of weighted samples without.
    pub(crate) fn new(lineless: bool) -> Self {
        Self {
            lineless,
            streams: foldhash::HashMap::default(),
            current: None,
            ready: Vec::new(),
            rows: 0,
        }
    }

    /// Appends a row of the stream `labels`; `weight` is ignored unless the
    /// batches are lineless.
    pub(crate) fn push<'m>(
        &mut self,
        labels: &Arc<Labels>,
        timestamp_ns: i64,
        line: &str,
        metadata: impl IntoIterator<Item = (&'m str, &'m str)>,
        weight: Option<(u32, u64)>,
    ) {
        let address = Arc::as_ptr(labels) as usize;
        let full = self
            .current
            .as_ref()
            .is_some_and(|(current, batch)| *current != address || batch.len() >= BATCH_ROWS);
        if full {
            self.flush();
        }
        let (rows, lineless) = (self.rows, self.lineless);
        let (_, batch) = self.current.get_or_insert_with(|| {
            let (_, map, stream) = self.streams.entry(address).or_insert_with(|| {
                let map: BTreeMap<String, String> = labels
                    .iter()
                    .map(|label| (label.name.clone(), label.value.clone()))
                    .collect();
                let stream = stream_hash(&map);
                (Arc::clone(labels), Arc::new(map), stream)
            });
            let tie = Tie {
                row: rows,
                ..Tie::default()
            };
            (
                address,
                ColumnBatch::new(Arc::clone(map), *stream, lineless, tie),
            )
        });
        batch.push(timestamp_ns, line, metadata, weight);
    }

    fn flush(&mut self) {
        if let Some((_, batch)) = self.current.take() {
            self.rows += batch.len() as u64;
            self.ready.push(BatchSlice::whole(batch));
        }
    }

    /// Every row pushed since the last call, as whole batches.
    pub(crate) fn take(&mut self) -> Vec<BatchSlice> {
        self.flush();
        std::mem::take(&mut self.ready)
    }
}

/// Reorders `values`, unless empty, so position `i` holds `order[i]`'s.
fn permute<T: Copy>(values: &mut Vec<T>, order: &[usize]) {
    if !values.is_empty() {
        *values = order.iter().map(|&row| values[row]).collect();
    }
}

fn offset(value: usize) -> u32 {
    u32::try_from(value).expect("a batch holds under 4 GiB")
}

pub(super) fn write_pair(hasher: &mut impl Hasher, name: &str, value: &str) {
    hasher.write(name.as_bytes());
    hasher.write_u8(0xff);
    hasher.write(value.as_bytes());
    hasher.write_u8(0xff);
}

/// A value's bytes: in the batch's buffer, or in the work arena when owned.
#[derive(Clone, Copy, Debug)]
struct Span {
    owned: bool,
    start: u32,
    end: u32,
}

/// One name's values across a slice.
#[derive(Clone)]
enum Column {
    /// The same value on every row.
    Constant(Span),
    Rows(Vec<Option<Span>>),
}

/// Labels or structured metadata of a slice's rows, by name in name order,
/// indexed by position in the slice.
#[derive(Clone, Default)]
pub(super) struct Fields(BTreeMap<Arc<str>, Column>);

impl Fields {
    fn get(&self, name: &str, at: usize) -> Option<Span> {
        match self.0.get(name)? {
            Column::Constant(span) => Some(*span),
            Column::Rows(values) => values[at],
        }
    }

    fn set(&mut self, name: &str, at: usize, rows: usize, value: Option<Span>) {
        let column = match self.0.get_mut(name) {
            Some(column) => column,
            None => {
                if value.is_none() {
                    return;
                }
                self.0
                    .entry(Arc::from(name))
                    .or_insert_with(|| Column::Rows(vec![None; rows]))
            }
        };
        if let Column::Constant(span) = column {
            *column = Column::Rows(vec![Some(*span); rows]);
        }
        let Column::Rows(values) = column else {
            unreachable!("made per-row above")
        };
        values[at] = value;
    }

    /// The fields at `at`, in name order.
    fn at(&self, at: usize) -> impl Iterator<Item = (&Arc<str>, Span)> {
        self.0.iter().filter_map(move |(name, column)| {
            let span = match column {
                Column::Constant(span) => *span,
                Column::Rows(values) => values[at]?,
            };
            Some((name, span))
        })
    }

    /// The slice's structured metadata as stored.
    pub(super) fn stored(slice: &BatchSlice) -> Self {
        let (batch, rows) = (&*slice.batch, slice.rows.clone());
        let mut fields = Self::default();
        for (at, row) in rows.clone().enumerate() {
            for &(name, start, end) in batch.metadata(row) {
                let span = Span {
                    owned: false,
                    start,
                    end,
                };
                let name = &batch.names[name as usize];
                if let Column::Rows(values) = fields
                    .0
                    .entry(Arc::clone(name))
                    .or_insert_with(|| Column::Rows(vec![None; rows.len()]))
                {
                    values[at] = Some(span);
                }
            }
        }
        fields
    }
}

/// One log expression's view of a slice as its stages run: the lines,
/// labels, metadata and unwrapped values they wrote, over the stored rows.
/// Rows are addressed by their index in the batch.
pub(super) struct Work<'b> {
    pub(super) batch: &'b ColumnBatch,
    base: usize,
    rows: usize,
    arena: String,
    /// Per row, once any stage rewrote a line.
    lines: Vec<Option<Span>>,
    /// Per row, once an `unwrap` ran.
    values: Vec<Option<f64>>,
    labels: Fields,
    metadata: Fields,
}

impl<'b> Work<'b> {
    pub(super) fn new(slice: &'b BatchSlice, metadata: &Fields) -> Self {
        let batch = &*slice.batch;
        let mut arena = String::new();
        let mut labels = Fields::default();
        for (name, value) in batch.labels.iter() {
            let span = push_owned(&mut arena, value);
            labels
                .0
                .insert(Arc::from(name.as_str()), Column::Constant(span));
        }
        Self {
            batch,
            base: slice.rows.start,
            rows: slice.len(),
            arena,
            lines: Vec::new(),
            values: Vec::new(),
            labels,
            metadata: metadata.clone(),
        }
    }

    fn str(&self, span: Span) -> &str {
        let text = if span.owned {
            &self.arena
        } else {
            &self.batch.text
        };
        &text[span.start as usize..span.end as usize]
    }

    pub(super) fn line(&self, row: usize) -> &str {
        match self.lines.get(row - self.base).copied().flatten() {
            Some(span) => self.str(span),
            None => self.batch.line(row),
        }
    }

    pub(super) fn value(&self, row: usize) -> Option<f64> {
        self.values.get(row - self.base).copied().flatten()
    }

    pub(super) fn label(&self, name: &str, row: usize) -> Option<&str> {
        self.labels
            .get(name, row - self.base)
            .map(|span| self.str(span))
    }

    fn metadata_value(&self, name: &str, row: usize) -> Option<&str> {
        self.metadata
            .get(name, row - self.base)
            .map(|span| self.str(span))
    }

    /// [`lookup`] without a cursor.
    pub(super) fn lookup(&self, name: &str, row: usize) -> Option<&str> {
        match name {
            "__line__" => Some(self.line(row)),
            _ => self
                .label(name, row)
                .or_else(|| self.metadata_value(name, row)),
        }
    }

    /// The row's labels in name order.
    pub(super) fn labels(&self, row: usize) -> impl Iterator<Item = (&str, &str)> {
        self.labels
            .at(row - self.base)
            .map(|(name, span)| (&**name, self.str(span)))
    }

    /// The row's structured metadata in name order.
    pub(super) fn metadata(&self, row: usize) -> impl Iterator<Item = (&str, &str)> {
        self.metadata
            .at(row - self.base)
            .map(|(name, span)| (&**name, self.str(span)))
    }

    pub(super) fn cursor(&mut self, row: usize) -> Cursor<'_, 'b> {
        Cursor { work: self, row }
    }

    /// Moves structured metadata into labels, as Loki does for metric
    /// queries: in name order, a name clashing with a label gets
    /// `_extracted`. The match score stays metadata.
    pub(super) fn promote_metadata(&mut self, rows: &[u32]) {
        let names = self
            .metadata
            .0
            .keys()
            .filter(|name| &***name != SCORE_METADATA_FIELD)
            .cloned()
            .collect::<Vec<_>>();
        if names.is_empty() {
            return;
        }
        let count = self.rows;
        for &row in rows {
            let at = row as usize - self.base;
            for name in &names {
                let Some(span) = self.metadata.get(name, at) else {
                    continue;
                };
                self.metadata.set(name, at, count, None);
                if self.labels.get(name, at).is_some() {
                    let extracted = format!("{name}_extracted");
                    self.labels.set(&extracted, at, count, Some(span));
                } else {
                    self.labels.set(name, at, count, Some(span));
                }
            }
        }
    }

    pub(super) fn metadata_map(&self, row: usize) -> BTreeMap<String, String> {
        self.metadata(row)
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect()
    }

    pub(super) fn has_error(&self, row: usize) -> bool {
        self.labels.get(ERROR_LABEL, row - self.base).is_some()
    }
}

fn push_owned(arena: &mut String, value: &str) -> Span {
    let start = offset(arena.len());
    arena.push_str(value);
    Span {
        owned: true,
        start,
        end: offset(arena.len()),
    }
}

/// One row of a [`Work`], as pipeline stages and templates see it.
pub(super) struct Cursor<'w, 'b> {
    work: &'w mut Work<'b>,
    row: usize,
}

impl Cursor<'_, '_> {
    fn at(&self) -> usize {
        self.row - self.work.base
    }

    /// Runs a `logfmt` stage without expressions as
    /// [`parse_stage`](super::super::pipeline::parse_stage) does, with values
    /// left in the batch's buffer unless unescaped; `false`, having done
    /// nothing, when the row's line is not the stored one.
    pub(super) fn logfmt(&mut self, strict: bool, keep_empty: bool) -> bool {
        let at = self.at();
        if self.work.lines.get(at).copied().flatten().is_some() {
            return false;
        }
        let batch = self.work.batch;
        let Some(&(line_start, line_end)) = batch.lines.get(self.row) else {
            return false;
        };
        let line = batch.span(line_start, line_end);
        let mut pairs = Vec::new();
        let error = logfmt_pairs(line, strict, |key, value| {
            if keep_empty || !value.is_empty() {
                pairs.push((key, value));
            }
        });
        // Name order, the first of each name kept, as a map would merge them.
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|later, first| later.0 == first.0);
        let rows = self.work.rows;
        for (key, value) in pairs {
            let clashes = self.work.labels.get(&key, at).is_some()
                || self.work.metadata.get(&key, at).is_some();
            let name = if clashes {
                // An empty extraction never displaces a clashing label's `_extracted` copy.
                if value.is_empty() {
                    continue;
                }
                Cow::Owned(format!("{key}_extracted"))
            } else {
                key
            };
            let span = match value {
                LogfmtValue::Range(range) => Span {
                    owned: false,
                    start: line_start + offset(range.start),
                    end: line_start + offset(range.end),
                },
                LogfmtValue::Owned(value) => push_owned(&mut self.work.arena, &value),
            };
            self.work.labels.set(&name, at, rows, Some(span));
        }
        if let Some(error) = error {
            set_error(self, "LogfmtParserErr", &error);
        }
        true
    }
}

impl Context for Cursor<'_, '_> {
    fn line(&self) -> &str {
        self.work.line(self.row)
    }

    fn timestamp_ns(&self) -> i64 {
        self.work.batch.timestamps[self.row]
    }

    fn get(&self, name: &str) -> Option<&str> {
        lookup(self, name)
    }

    fn entries(&self) -> BTreeMap<String, String> {
        let mut entries = self.work.metadata_map(self.row);
        entries.extend(
            self.work
                .labels(self.row)
                .map(|(name, value)| (name.to_owned(), value.to_owned())),
        );
        entries
    }
}

impl StageRow for Cursor<'_, '_> {
    fn set_line(&mut self, line: String) {
        let (at, rows) = (self.at(), self.work.rows);
        let span = push_owned(&mut self.work.arena, &line);
        let lines = &mut self.work.lines;
        if lines.is_empty() {
            lines.resize(rows, None);
        }
        lines[at] = Some(span);
    }

    fn label(&self, name: &str) -> Option<&str> {
        self.work.label(name, self.row)
    }

    fn metadata_value(&self, name: &str) -> Option<&str> {
        self.work.metadata_value(name, self.row)
    }

    fn insert_label(&mut self, name: String, value: String) {
        let (at, rows) = (self.at(), self.work.rows);
        let span = push_owned(&mut self.work.arena, &value);
        self.work.labels.set(&name, at, rows, Some(span));
    }

    fn remove_label(&mut self, name: &str) -> Option<String> {
        let removed = self.label(name)?.to_owned();
        let (at, rows) = (self.at(), self.work.rows);
        self.work.labels.set(name, at, rows, None);
        Some(removed)
    }

    fn remove_metadata(&mut self, name: &str) -> Option<String> {
        let removed = self.metadata_value(name)?.to_owned();
        let (at, rows) = (self.at(), self.work.rows);
        self.work.metadata.set(name, at, rows, None);
        Some(removed)
    }

    fn retain_fields(&mut self, keep: &mut dyn FnMut(&str, &str) -> bool) {
        let (at, rows) = (self.at(), self.work.rows);
        for metadata in [true, false] {
            let work = &*self.work;
            let source = if metadata {
                &work.metadata
            } else {
                &work.labels
            };
            let removed = source
                .at(at)
                .filter(|(name, span)| !keep(name, work.str(*span)))
                .map(|(name, _)| Arc::clone(name))
                .collect::<Vec<_>>();
            let target = if metadata {
                &mut self.work.metadata
            } else {
                &mut self.work.labels
            };
            for name in removed {
                target.set(&name, at, rows, None);
            }
        }
    }

    fn set_value(&mut self, value: f64) {
        let (at, rows) = (self.at(), self.work.rows);
        let values = &mut self.work.values;
        if values.is_empty() {
            values.resize(rows, None);
        }
        values[at] = Some(value);
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::pipeline::parse_stage;
    use super::*;
    use crate::logql::ParserStage;

    fn batch(lines: &[&str]) -> BatchSlice {
        let labels = BTreeMap::from([
            ("app".to_owned(), "api".to_owned()),
            ("level".to_owned(), "info".to_owned()),
        ]);
        let mut batch = ColumnBatch::new(Arc::new(labels), 0, false, Tie::default());
        for (index, line) in lines.iter().enumerate() {
            batch.push(index as i64, line, [("trace_id", "abc")], None);
        }
        BatchSlice::whole(batch)
    }

    #[test]
    fn logfmt_fast_path_matches_the_row_parser() {
        let lines = [
            "level=warn msg=\"a \\\"quoted\\\" value\" took=5ms",
            "a=1 a=2 b= c b=3",
            "app=web app_extracted=x trace_id=t trace_id=",
            "service.name=x 9lives=y é=z",
            "msg=\"unterminated",
            "k=v=w x=\"y\"z",
            "=bad \"quoted\" key=value",
            "",
        ];
        let slice = batch(&lines);
        let stored = Fields::stored(&slice);
        for strict in [false, true] {
            for keep_empty in [false, true] {
                let parser = ParserStage::Logfmt {
                    strict,
                    keep_empty,
                    expressions: Vec::new(),
                };
                let (mut fast, mut slow) = (Work::new(&slice, &stored), Work::new(&slice, &stored));
                for (row, line) in lines.iter().enumerate() {
                    assert!(fast.cursor(row).logfmt(strict, keep_empty));
                    parse_stage(&mut slow.cursor(row), &parser).unwrap();
                    let labels = |work: &Work<'_>| {
                        work.labels(row)
                            .map(|(name, value)| (name.to_owned(), value.to_owned()))
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(
                        labels(&fast),
                        labels(&slow),
                        "{line:?} strict {strict} keep_empty {keep_empty}"
                    );
                }
            }
        }
    }
}
