// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! Path globbing and content search over a tenant filesystem snapshot.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::BytesRange;
use common::storage::StorageSnapshot;
use futures::StreamExt;
use regex::bytes::{Regex, RegexBuilder};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::codec::{TenantScope, directory_entry_key, directory_prefix, inode_key};
use crate::db::{Caching, Child, PseudoFs, components, decode_child, join_path, normalize_path};
use crate::error::{Error, Result};
use crate::model::{DirectoryEntry, FileKind, Generation, Inode};

const BINARY_PROBE_BYTES: usize = 8 * 1024;
const MAX_LINE_BYTES: usize = 1024 * 1024;
const MAX_LINE_DISPLAY_BYTES: usize = 4096;
const REGEX_SIZE_LIMIT: usize = 10 * 1024 * 1024;
/// Per-search lazy DFA cache. The 2 MiB default thrashes on large counted
/// repetitions such as `x{4000}`, falling back to an engine three orders of
/// magnitude slower; the cache only grows as a pattern needs it.
const REGEX_DFA_SIZE_LIMIT: usize = 16 * 1024 * 1024;
const SCAN_YIELD_BYTES: usize = 256 * 1024;
/// How many directory entries a walk reads between checks that its receiver
/// is still listening, so cancelled walks stop even where nothing matches.
const CANCEL_CHECK_ENTRIES: usize = 256;

/// Extensions of formats that are never worth searching as text. Files with
/// these extensions are skipped without reading their contents.
const BINARY_EXTENSIONS: &[&str] = &[
    "png",
    "jpg",
    "jpeg",
    "gif",
    "webp",
    "bmp",
    "ico",
    "tif",
    "tiff",
    "heic",
    "heif",
    "avif",
    "psd",
    "raw",
    "cr2",
    "nef",
    "pdf",
    "zip",
    "gz",
    "tgz",
    "bz2",
    "xz",
    "zst",
    "lz4",
    "7z",
    "rar",
    "tar",
    "jar",
    "war",
    "whl",
    "apk",
    "dmg",
    "iso",
    "docx",
    "xlsx",
    "pptx",
    "odt",
    "ods",
    "odp",
    "exe",
    "dll",
    "so",
    "dylib",
    "o",
    "a",
    "lib",
    "obj",
    "class",
    "pyc",
    "pyo",
    "wasm",
    "mp3",
    "mp4",
    "m4a",
    "m4v",
    "mov",
    "avi",
    "mkv",
    "webm",
    "wav",
    "flac",
    "ogg",
    "opus",
    "aac",
    "wma",
    "wmv",
    "woff",
    "woff2",
    "ttf",
    "otf",
    "eot",
    "sqlite",
    "sqlite3",
    "parquet",
    "avro",
    "orc",
    "arrow",
    "feather",
    "npy",
    "npz",
    "pkl",
    "pickle",
    "h5",
    "hdf5",
    "onnx",
    "pt",
    "pth",
    "safetensors",
];

/// Leading bytes of binary formats, for files without a telling extension.
const BINARY_SIGNATURES: &[&[u8]] = &[
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",
    b"GIF87a",
    b"GIF89a",
    b"RIFF",
    b"II*\x00",
    b"MM\x00*",
    b"%PDF-",
    b"PK\x03\x04",
    b"PK\x05\x06",
    b"\x1f\x8b",
    b"\xfd7zXZ\x00",
    b"\x28\xb5\x2f\xfd",
    b"7z\xbc\xaf\x27\x1c",
    b"Rar!\x1a\x07",
    b"\x7fELF",
    b"\xcf\xfa\xed\xfe",
    b"\xce\xfa\xed\xfe",
    b"\xca\xfe\xba\xbe",
    b"\x00asm",
    b"OggS",
    b"fLaC",
    b"SQLite format 3\x00",
    b"PAR1",
    b"wOFF",
    b"wOF2",
];

#[derive(Clone, Debug)]
pub struct GlobOptions {
    /// Stop after this many matches.
    pub max_results: usize,
    /// Fail with [`Error::ResourceExhausted`] after reading this many
    /// directory entries, bounding patterns like `/**/x` over huge trees.
    pub max_scanned_entries: usize,
}

impl Default for GlobOptions {
    fn default() -> Self {
        Self {
            max_results: 1000,
            max_scanned_entries: 1_000_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrepOptions {
    pub pattern: String,
    /// Treat `pattern` as a literal string rather than a regular expression.
    pub fixed_string: bool,
    pub case_insensitive: bool,
    pub before_context: usize,
    pub after_context: usize,
    /// Stop after this many matching lines across all files.
    pub max_matches: usize,
    /// Skip files larger than this.
    pub max_file_size_bytes: u64,
    /// Files searched at once. Results are still returned in walk order.
    pub concurrency: usize,
    /// Fail with [`Error::ResourceExhausted`] after the file walk reads this
    /// many directory entries.
    pub max_scanned_entries: usize,
    /// Fail with [`Error::ResourceExhausted`] after searching this long. Time
    /// spent waiting for the receiver to take matches does not count.
    pub timeout: Duration,
}

impl Default for GrepOptions {
    fn default() -> Self {
        Self {
            pattern: String::new(),
            fixed_string: false,
            case_insensitive: false,
            before_context: 0,
            after_context: 0,
            max_matches: 100,
            max_file_size_bytes: 16 * 1024 * 1024,
            concurrency: 8,
            max_scanned_entries: 1_000_000,
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrepLine {
    pub line_number: u64,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrepMatch {
    pub path: String,
    pub line_number: u64,
    pub line: String,
    pub before: Vec<GrepLine>,
    pub after: Vec<GrepLine>,
}

enum Segment {
    Literal(String),
    Pattern(regex::Regex),
    Recursive,
}

/// A compiled glob. `*`, `?`, `[...]`, and `{a,b}` match within one path
/// component; a `**` component matches zero or more directories.
struct GlobPattern {
    segments: Vec<Segment>,
    /// Directory named by the pattern's leading literal components.
    base: String,
    /// Index of the first segment after `base`.
    start: usize,
}

impl GlobPattern {
    fn compile(pattern: &str) -> Result<Self> {
        let normalized = normalize_path(pattern)?;
        let mut segments: Vec<Segment> = Vec::new();
        for component in components(&normalized) {
            let segment = if component == "**" {
                if matches!(segments.last(), Some(Segment::Recursive)) {
                    continue;
                }
                Segment::Recursive
            } else if has_glob_meta(component) {
                Segment::Pattern(component_regex(component)?)
            } else {
                Segment::Literal(unescape(component))
            };
            segments.push(segment);
        }
        let start = segments
            .iter()
            .position(|segment| !matches!(segment, Segment::Literal(_)))
            .unwrap_or(segments.len());
        let base = format!(
            "/{}",
            segments[..start]
                .iter()
                .filter_map(|segment| match segment {
                    Segment::Literal(literal) => Some(literal.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("/")
        );
        Ok(Self {
            segments,
            base,
            start,
        })
    }

    /// Everything below the normalized directory `base`, whose components are
    /// literal names rather than glob syntax.
    fn below(base: String) -> Self {
        Self {
            segments: vec![Segment::Recursive],
            base,
            start: 0,
        }
    }

    fn initial(&self) -> Vec<usize> {
        self.closure(vec![self.start])
    }

    fn closure(&self, mut states: Vec<usize>) -> Vec<usize> {
        let mut index = 0;
        while index < states.len() {
            let state = states[index];
            if matches!(self.segments.get(state), Some(Segment::Recursive))
                && !states.contains(&(state + 1))
            {
                states.push(state + 1);
            }
            index += 1;
        }
        states.sort_unstable();
        states.dedup();
        states
    }

    fn step(&self, states: &[usize], name: &str) -> Vec<usize> {
        let mut next = Vec::new();
        for &state in states {
            match self.segments.get(state) {
                Some(Segment::Recursive) => next.push(state),
                Some(Segment::Literal(literal)) if literal == name => next.push(state + 1),
                Some(Segment::Pattern(regex)) if regex.is_match(name) => next.push(state + 1),
                _ => {}
            }
        }
        self.closure(next)
    }

    fn accepts(&self, states: &[usize]) -> bool {
        states.contains(&self.segments.len())
    }

    fn can_descend(&self, states: &[usize]) -> bool {
        states.iter().any(|&state| state < self.segments.len())
    }

    /// The only child names that can advance `states`, when every pending
    /// segment is a literal. Such directories are probed by key instead of
    /// scanned.
    fn literal_names(&self, states: &[usize]) -> Option<Vec<&str>> {
        let mut names = Vec::new();
        for &state in states {
            match self.segments.get(state) {
                Some(Segment::Literal(literal)) => names.push(literal.as_str()),
                Some(_) => return None,
                None => {}
            }
        }
        names.sort_unstable();
        names.dedup();
        Some(names)
    }
}

struct Found {
    entry: DirectoryEntry,
    id: Uuid,
}

impl PseudoFs {
    /// Streams entries whose paths match `pattern`, walking only directories
    /// the pattern can still match. Directories whose remaining pattern
    /// component is a literal are probed by key rather than listed. Stops
    /// after `max_results` entries or when the receiver is dropped.
    pub async fn glob(
        &self,
        tenant: &str,
        pattern: &str,
        options: GlobOptions,
    ) -> Result<mpsc::Receiver<Result<DirectoryEntry>>> {
        let pattern = GlobPattern::compile(pattern)?;
        let scope = self.ensure_tenant(tenant).await?;
        let snapshot = self.inner.storage.snapshot().await?;
        let mut found = self
            .walk(
                tenant,
                scope,
                snapshot,
                pattern,
                options.max_scanned_entries,
            )
            .await?;
        let (sender, receiver) = mpsc::channel(32);
        let max_results = options.max_results;
        tokio::spawn(async move {
            let mut sent = 0;
            while sent < max_results {
                let Some(item) = found.recv().await else {
                    return;
                };
                let failed = item.is_err();
                if sender.send(item.map(|found| found.entry)).await.is_err() || failed {
                    return;
                }
                sent += 1;
            }
        });
        Ok(receiver)
    }

    /// Searches file contents. `path` is a glob, a file, or a directory
    /// searched recursively. Up to `concurrency` files are searched at once,
    /// and matches are returned in walk order. Binary files are skipped: known
    /// binary extensions without reading them, and otherwise files whose first
    /// 8 KiB contain a NUL byte or a binary format signature. Files over
    /// `max_file_size_bytes` and the rest of a file after a line over 1 MiB
    /// are also skipped.
    ///
    /// The match budget is shared by the files being searched, so when
    /// `max_matches` is reached the returned matches may include later files
    /// in place of earlier ones that were still being read.
    pub async fn grep(
        &self,
        tenant: &str,
        path: &str,
        options: GrepOptions,
    ) -> Result<mpsc::Receiver<Result<GrepMatch>>> {
        let pattern = if options.fixed_string {
            regex::escape(&options.pattern)
        } else {
            options.pattern.clone()
        };
        let regex = RegexBuilder::new(&pattern)
            .case_insensitive(options.case_insensitive)
            .size_limit(REGEX_SIZE_LIMIT)
            .dfa_size_limit(REGEX_DFA_SIZE_LIMIT)
            .build()
            .map_err(|error| Error::InvalidPattern(error.to_string()))?;
        let scope = self.ensure_tenant(tenant).await?;
        let snapshot = self.inner.storage.snapshot().await?;
        let max_scanned = options.max_scanned_entries;
        let mut found = if has_glob_meta(path) {
            self.walk(
                tenant,
                scope,
                Arc::clone(&snapshot),
                GlobPattern::compile(path)?,
                max_scanned,
            )
            .await?
        } else {
            let normalized = normalize_path(path)?;
            let inode = self
                .resolve_from(snapshot.as_ref(), tenant, scope, &normalized)
                .await?;
            if inode.kind == FileKind::File {
                let (sender, receiver) = mpsc::channel(1);
                let name = normalized.rsplit('/').next().unwrap_or_default().to_owned();
                let _ = sender
                    .send(Ok(Found {
                        entry: DirectoryEntry {
                            path: normalized,
                            name,
                            kind: FileKind::File,
                        },
                        id: inode.id,
                    }))
                    .await;
                receiver
            } else {
                let recursive = GlobPattern::below(normalized);
                self.walk(tenant, scope, Arc::clone(&snapshot), recursive, max_scanned)
                    .await?
            }
        };
        let (sender, receiver) = mpsc::channel(32);
        let fs = self.clone();
        tokio::spawn(async move {
            let mut remaining = options.max_matches;
            if remaining == 0 {
                return;
            }
            let concurrency = options.concurrency.max(1);
            let timeout = options.timeout;
            let budget = Arc::new(AtomicUsize::new(remaining));
            let options = Arc::new(options);
            let receiver_gone = sender.clone();
            let files = futures::stream::poll_fn(move |cx| found.poll_recv(cx)).filter_map(
                |item| async move {
                    match item {
                        Ok(file) if file.entry.kind == FileKind::File => Some(Ok(file)),
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    }
                },
            );
            let searches = files
                .map(|file| {
                    let fs = fs.clone();
                    let snapshot = Arc::clone(&snapshot);
                    let regex = regex.clone();
                    let options = Arc::clone(&options);
                    let budget = Arc::clone(&budget);
                    let receiver_gone = receiver_gone.clone();
                    async move {
                        let cancelled = || receiver_gone.is_closed();
                        fs.grep_file(
                            snapshot, scope, &file?, &regex, &options, &budget, &cancelled,
                        )
                        .await
                    }
                })
                .buffered(concurrency);
            let mut searches = std::pin::pin!(searches);
            let mut searching = timeout;
            loop {
                let started = tokio::time::Instant::now();
                let Ok(next) = tokio::time::timeout(searching, searches.next()).await else {
                    let _ = sender
                        .send(Err(Error::ResourceExhausted(format!(
                            "grep searched for longer than {timeout:?}; narrow the path or pattern"
                        ))))
                        .await;
                    return;
                };
                searching = searching.saturating_sub(started.elapsed());
                let Some(searched) = next else {
                    return;
                };
                let matches = match searched {
                    Ok(matches) => matches,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                for found in matches {
                    if sender.send(Ok(found)).await.is_err() {
                        return;
                    }
                    remaining -= 1;
                    if remaining == 0 {
                        return;
                    }
                }
            }
        });
        Ok(receiver)
    }

    async fn walk(
        &self,
        tenant: &str,
        scope: TenantScope,
        snapshot: Arc<dyn StorageSnapshot>,
        pattern: GlobPattern,
        max_scanned: usize,
    ) -> Result<mpsc::Receiver<Result<Found>>> {
        let (sender, receiver) = mpsc::channel(32);
        let base = match self
            .resolve_from(snapshot.as_ref(), tenant, scope, &pattern.base)
            .await
        {
            Ok(inode) => inode,
            Err(Error::NotFound(_) | Error::NotDirectory(_)) => return Ok(receiver),
            Err(error) => return Err(error),
        };
        if pattern.start == pattern.segments.len() {
            if pattern.base != "/" {
                let name = pattern
                    .base
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .to_owned();
                let _ = sender
                    .send(Ok(Found {
                        entry: DirectoryEntry {
                            path: pattern.base.clone(),
                            name,
                            kind: base.kind,
                        },
                        id: base.id,
                    }))
                    .await;
            }
            return Ok(receiver);
        }
        if base.kind != FileKind::Directory {
            return Ok(receiver);
        }
        tokio::spawn(async move {
            let initial = pattern.initial();
            let mut stack = vec![(base.id, pattern.base.clone(), initial)];
            let mut walk = WalkBudget {
                scanned: 0,
                max_scanned,
            };
            while let Some((directory, path, states)) = stack.pop() {
                if sender.is_closed() {
                    return;
                }
                let walked = walk_directory(
                    snapshot.as_ref(),
                    scope,
                    &pattern,
                    directory,
                    &path,
                    &states,
                    &sender,
                    &mut walk,
                )
                .await;
                match walked {
                    Ok(Some(subdirectories)) => stack.extend(subdirectories.into_iter().rev()),
                    Ok(None) => return,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
        });
        Ok(receiver)
    }

    /// Greps one file, claiming matches from the shared `budget`.
    #[allow(clippy::too_many_arguments)]
    async fn grep_file(
        &self,
        snapshot: Arc<dyn StorageSnapshot>,
        scope: TenantScope,
        file: &Found,
        regex: &Regex,
        options: &GrepOptions,
        budget: &AtomicUsize,
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<Vec<GrepMatch>> {
        if budget.load(Ordering::Acquire) == 0
            || has_binary_extension(&file.entry.name)
            || cancelled()
        {
            return Ok(Vec::new());
        }
        let Some(record) = snapshot.get(inode_key(scope, file.id)).await? else {
            return Ok(Vec::new());
        };
        let inode: Inode = crate::codec::decode(&record.value)?;
        if inode.size == 0 || inode.size > options.max_file_size_bytes {
            return Ok(Vec::new());
        }
        let generations = self
            .generation_chain(snapshot.as_ref(), scope, inode.id, inode.head)
            .await?;
        let probe_len = inode.size.min(BINARY_PROBE_BYTES as u64);
        let probe = self
            .content_stream(
                Arc::clone(&snapshot),
                scope,
                generations.clone(),
                0..probe_len,
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Bytes>>>()?
            .concat();
        if looks_binary(&probe) {
            return Ok(Vec::new());
        }
        let content = if probe_len == inode.size {
            futures::stream::iter([Ok(Bytes::from(probe))]).boxed()
        } else {
            self.content_stream(snapshot, scope, generations, 0..u64::MAX)
                .boxed()
        };
        scan_lines(
            content,
            LineScanner::new(&file.entry.path, regex, options, budget),
            cancelled,
        )
        .await
    }

    /// Streams the bytes of `range` from `generations`.
    fn content_stream(
        &self,
        snapshot: Arc<dyn StorageSnapshot>,
        scope: TenantScope,
        generations: Vec<Generation>,
        range: Range<u64>,
    ) -> impl futures::Stream<Item = Result<Bytes>> + Send + 'static {
        let (sender, mut receiver) = mpsc::channel::<Result<Bytes>>(2);
        let reader = self.clone();
        tokio::spawn(async move {
            if let Err(error) = reader
                .send_content(
                    snapshot,
                    scope,
                    &generations,
                    range,
                    &sender,
                    Caching::Bypass,
                )
                .await
            {
                let _ = sender.send(Err(error)).await;
            }
        });
        futures::stream::poll_fn(move |cx| receiver.poll_recv(cx))
    }
}

/// Matches `content` line by line. Yields to the runtime every
/// `SCAN_YIELD_BYTES`, so slow patterns cannot hold a worker thread for a whole
/// chunk, and stops once `cancelled` reports the client is gone.
async fn scan_lines(
    mut content: futures::stream::BoxStream<'static, Result<Bytes>>,
    mut scanner: LineScanner<'_>,
    cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<Vec<GrepMatch>> {
    let mut carry: Vec<u8> = Vec::new();
    let mut output = Vec::new();
    let mut since_yield = 0;
    while let Some(chunk) = content.next().await {
        carry.extend_from_slice(&chunk?);
        let mut start = 0;
        while let Some(newline) = memchr::memchr(b'\n', &carry[start..]) {
            scanner.line(&carry[start..start + newline], &mut output);
            start += newline + 1;
            if scanner.done() {
                break;
            }
            since_yield += newline + 1;
            if since_yield >= SCAN_YIELD_BYTES {
                since_yield = 0;
                tokio::task::yield_now().await;
                if cancelled() {
                    return Ok(output);
                }
            }
        }
        carry.drain(..start);
        if scanner.done() {
            scanner.finish(&mut output);
            return Ok(output);
        }
        if carry.len() > MAX_LINE_BYTES {
            scanner.finish(&mut output);
            return Ok(output);
        }
    }
    if !carry.is_empty() {
        scanner.line(&carry, &mut output);
    }
    scanner.finish(&mut output);
    Ok(output)
}

fn has_binary_extension(name: &str) -> bool {
    name.rsplit_once('.').is_some_and(|(stem, extension)| {
        !stem.is_empty()
            && BINARY_EXTENSIONS
                .iter()
                .any(|binary| binary.eq_ignore_ascii_case(extension))
    })
}

fn looks_binary(probe: &[u8]) -> bool {
    memchr::memchr(0, probe).is_some()
        || BINARY_SIGNATURES
            .iter()
            .any(|signature| probe.starts_with(signature))
        || probe.get(4..8) == Some(&b"ftyp"[..])
}

struct WalkBudget {
    scanned: usize,
    max_scanned: usize,
}

impl WalkBudget {
    fn record(&mut self) -> Result<()> {
        self.scanned += 1;
        if self.scanned > self.max_scanned {
            return Err(Error::ResourceExhausted(format!(
                "pattern walk read more than {} directory entries; narrow the pattern",
                self.max_scanned
            )));
        }
        Ok(())
    }
}

/// Lists one directory, sending matches. Returns the subdirectories still
/// worth visiting, or `None` once the receiver is gone. When every pending
/// pattern component is a literal, only those names are looked up.
#[allow(clippy::too_many_arguments)]
async fn walk_directory(
    snapshot: &dyn StorageSnapshot,
    scope: TenantScope,
    pattern: &GlobPattern,
    directory: Uuid,
    path: &str,
    states: &[usize],
    sender: &mpsc::Sender<Result<Found>>,
    budget: &mut WalkBudget,
) -> Result<Option<Vec<(Uuid, String, Vec<usize>)>>> {
    let mut subdirectories = Vec::new();
    if let Some(names) = pattern.literal_names(states) {
        for name in names {
            budget.record()?;
            let Some(record) = snapshot
                .get(directory_entry_key(scope, directory, name))
                .await?
            else {
                continue;
            };
            let child = decode_child(&record, scope)?;
            if !visit(pattern, path, states, child, sender, &mut subdirectories).await {
                return Ok(None);
            }
        }
        return Ok(Some(subdirectories));
    }
    let mut iter = snapshot
        .scan_prefix_iter(
            directory_prefix(scope, directory),
            BytesRange::unbounded(),
            None,
        )
        .await?;
    while let Some(record) = iter.next().await? {
        budget.record()?;
        if budget.scanned.is_multiple_of(CANCEL_CHECK_ENTRIES) && sender.is_closed() {
            return Ok(None);
        }
        let child = decode_child(&record, scope)?;
        if !visit(pattern, path, states, child, sender, &mut subdirectories).await {
            return Ok(None);
        }
    }
    Ok(Some(subdirectories))
}

/// Advances the pattern over one child, sending it if it matches and queueing
/// it if it may contain matches. Returns `false` once the receiver is gone.
async fn visit(
    pattern: &GlobPattern,
    path: &str,
    states: &[usize],
    child: Child,
    sender: &mpsc::Sender<Result<Found>>,
    subdirectories: &mut Vec<(Uuid, String, Vec<usize>)>,
) -> bool {
    let next = pattern.step(states, &child.name);
    if next.is_empty() {
        return true;
    }
    let child_path = join_path(path, &child.name);
    if pattern.accepts(&next) {
        let found = Found {
            entry: DirectoryEntry {
                path: child_path.clone(),
                name: child.name,
                kind: child.kind,
            },
            id: child.id,
        };
        if sender.send(Ok(found)).await.is_err() {
            return false;
        }
    }
    if child.kind == FileKind::Directory && pattern.can_descend(&next) {
        subdirectories.push((child.id, child_path, next));
    }
    true
}

struct LineScanner<'a> {
    path: &'a str,
    regex: &'a Regex,
    before_context: usize,
    after_context: usize,
    budget: &'a AtomicUsize,
    exhausted: bool,
    line_number: u64,
    before: VecDeque<GrepLine>,
    pending: VecDeque<(GrepMatch, usize)>,
}

impl<'a> LineScanner<'a> {
    fn new(
        path: &'a str,
        regex: &'a Regex,
        options: &GrepOptions,
        budget: &'a AtomicUsize,
    ) -> Self {
        Self {
            path,
            regex,
            before_context: options.before_context,
            after_context: options.after_context,
            budget,
            exhausted: false,
            line_number: 0,
            before: VecDeque::with_capacity(options.before_context),
            pending: VecDeque::new(),
        }
    }

    fn claim(&mut self) -> bool {
        if self.exhausted {
            return false;
        }
        let claimed = self
            .budget
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        self.exhausted = !claimed;
        claimed
    }

    fn line(&mut self, raw: &[u8], output: &mut Vec<GrepMatch>) {
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        self.line_number += 1;
        let line = GrepLine {
            line_number: self.line_number,
            text: display_line(raw),
        };
        for (_, wanted) in &mut self.pending {
            *wanted -= 1;
        }
        for (found, _) in &mut self.pending {
            found.after.push(line.clone());
        }
        while self.pending.front().is_some_and(|(_, wanted)| *wanted == 0) {
            if let Some((found, _)) = self.pending.pop_front() {
                output.push(found);
            }
        }
        if !self.exhausted && self.regex.is_match(raw) && self.claim() {
            let found = GrepMatch {
                path: self.path.to_owned(),
                line_number: self.line_number,
                line: line.text.clone(),
                before: self.before.iter().cloned().collect(),
                after: Vec::new(),
            };
            if self.after_context == 0 {
                output.push(found);
            } else {
                self.pending.push_back((found, self.after_context));
            }
        }
        if self.before_context > 0 {
            if self.before.len() == self.before_context {
                self.before.pop_front();
            }
            self.before.push_back(line);
        }
    }

    fn done(&self) -> bool {
        (self.exhausted || self.budget.load(Ordering::Acquire) == 0) && self.pending.is_empty()
    }

    fn finish(&mut self, output: &mut Vec<GrepMatch>) {
        output.extend(self.pending.drain(..).map(|(found, _)| found));
    }
}

fn display_line(raw: &[u8]) -> String {
    let raw = &raw[..raw.len().min(MAX_LINE_DISPLAY_BYTES)];
    String::from_utf8_lossy(raw).into_owned()
}

pub(crate) fn has_glob_meta(value: &str) -> bool {
    value.contains(['*', '?', '[', '{'])
}

fn unescape(component: &str) -> String {
    let mut output = String::with_capacity(component.len());
    let mut chars = component.chars();
    while let Some(char) = chars.next() {
        if char == '\\' {
            if let Some(next) = chars.next() {
                output.push(next);
            }
        } else {
            output.push(char);
        }
    }
    output
}

fn component_regex(component: &str) -> Result<regex::Regex> {
    let body = translate(component, false)?;
    regex::Regex::new(&format!("^(?s:{body})$"))
        .map_err(|error| Error::InvalidPattern(error.to_string()))
}

fn translate(glob: &str, in_braces: bool) -> Result<String> {
    let chars: Vec<char> = glob.chars().collect();
    let mut output = String::new();
    let mut index = 0;
    while index < chars.len() {
        match chars[index] {
            '*' => output.push_str(".*"),
            '?' => output.push('.'),
            '\\' => {
                index += 1;
                if let Some(next) = chars.get(index) {
                    output.push_str(&regex::escape(&next.to_string()));
                }
            }
            '[' => match chars[index + 1..].iter().position(|&c| c == ']') {
                Some(offset) if offset > 0 => {
                    let class: String = chars[index + 1..index + 1 + offset].iter().collect();
                    let class = match class.strip_prefix(['!', '^']) {
                        Some(negated) => format!("^{}", negated.replace('\\', "\\\\")),
                        None => class.replace('\\', "\\\\"),
                    };
                    output.push('[');
                    output.push_str(&class);
                    output.push(']');
                    index += offset + 1;
                }
                _ => output.push_str("\\["),
            },
            '{' if !in_braces => {
                let close = chars[index + 1..]
                    .iter()
                    .position(|&c| c == '}')
                    .ok_or_else(|| Error::InvalidPattern(format!("unclosed '{{' in {glob}")))?;
                let inner: String = chars[index + 1..index + 1 + close].iter().collect();
                let alternatives = inner
                    .split(',')
                    .map(|alternative| translate(alternative, true))
                    .collect::<Result<Vec<_>>>()?;
                output.push_str("(?:");
                output.push_str(&alternatives.join("|"));
                output.push(')');
                index += close + 1;
            }
            other => output.push_str(&regex::escape(&other.to_string())),
        }
        index += 1;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(pattern: &str, path: &str) -> bool {
        let glob = GlobPattern::compile(pattern).unwrap();
        let mut states = glob.closure(vec![0]);
        for component in components(path) {
            states = glob.step(&states, component);
            if states.is_empty() {
                return false;
            }
        }
        glob.accepts(&states)
    }

    #[test]
    fn glob_components_and_recursion() {
        assert!(matches("/src/*.rs", "/src/lib.rs"));
        assert!(!matches("/src/*.rs", "/src/nested/lib.rs"));
        assert!(matches("/src/**/*.rs", "/src/lib.rs"));
        assert!(matches("/src/**/*.rs", "/src/a/b/lib.rs"));
        assert!(matches("/**/subfolder", "/dir/x/subfolder"));
        assert!(matches("/dir/**/subfolder", "/dir/subfolder"));
        assert!(!matches("/dir/**/subfolder", "/other/subfolder"));
        assert!(matches("/dir/**", "/dir/a/b"));
        assert!(matches("/img/*.{png,jpg}", "/img/a.jpg"));
        assert!(!matches("/img/*.{png,jpg}", "/img/a.gif"));
        assert!(matches("/f?le[0-9].txt", "/file7.txt"));
        assert!(!matches("/file[!0-9].txt", "/file7.txt"));
        assert!(matches("/literal\\*.txt", "/literal*.txt"));
    }

    #[test]
    fn classifies_binary_files() {
        assert!(has_binary_extension("photo.JPG"));
        assert!(has_binary_extension("archive.tar.gz"));
        assert!(!has_binary_extension("notes.txt"));
        assert!(!has_binary_extension(".png"));
        assert!(!has_binary_extension("Makefile"));
        assert!(looks_binary(b"\x89PNG\r\n\x1a\n...."));
        assert!(looks_binary(b"\x00\x00\x00\x20ftypheic"));
        assert!(looks_binary(b"text with a \0 byte"));
        assert!(!looks_binary(b"plain text\nwith lines\n"));
    }

    #[test]
    fn literal_components_are_probed_by_name() {
        let glob = GlobPattern::compile("/a/*/{x,y}/z").unwrap();
        let states = glob.step(&glob.initial(), "b");
        assert_eq!(glob.literal_names(&states), None);
        let glob = GlobPattern::compile("/*/sub/file").unwrap();
        let states = glob.step(&glob.initial(), "dir");
        assert_eq!(glob.literal_names(&states), Some(vec!["sub"]));
        let recursive = GlobPattern::compile("/**/sub").unwrap();
        assert_eq!(recursive.literal_names(&recursive.initial()), None);
    }

    #[test]
    fn glob_base_skips_literal_prefix() {
        let glob = GlobPattern::compile("/a/b/**/c/*.txt").unwrap();
        assert_eq!(glob.base, "/a/b");
        assert_eq!(glob.start, 2);
        assert!(GlobPattern::compile("/a/{b").is_err());
    }
}
