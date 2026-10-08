//! Query latency against accumulated data, modelled on the differential
//! fuzz stack: a writer ingests a fresh "round" of series per iteration
//! (new `fuzz_round` value, same `fuzz_run`) and a separate `DbReader`-backed
//! reader answers queries pinned to the latest round, so the matched series
//! count stays constant while the bucket keeps growing. Latency that tracks
//! selection size stays flat across rounds. Not a criterion bench.
//!
//! ```text
//! cargo bench -p plural-metrics --features bench-internals --bench query_growth -- \
//!     [--rounds N] [--series N] [--samples N] [--iters N] [--every N] [--poll-ms N]
//! ```

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::storage::config::{
    BlockCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
    SlateDbStorageConfig,
};
use plural_metrics::{Config, Label, Namespace, Sample, Series, TimeSeriesDb, TimeSeriesDbReader};
use slatedb::config::DbReaderOptions;

const HOUR_MS: i64 = 60 * 60 * 1000;
const T0_MS: i64 = 1_699_999_200_000;
const WINDOW_MS: i64 = 30 * 60 * 1000;
const RUN: &str = "growth-run";

struct Args {
    rounds: usize,
    series: usize,
    samples: usize,
    iters: usize,
    every: usize,
    poll_ms: u64,
    hold_secs: u64,
    reopen: bool,
}

fn args() -> Args {
    let mut args = Args {
        rounds: 150,
        series: 100,
        samples: 30,
        iters: 20,
        every: 10,
        poll_ms: 200,
        hold_secs: 0,
        reopen: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || -> usize {
            it.next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("{flag} needs a number"))
        };
        match flag.as_str() {
            "--rounds" => args.rounds = value(),
            "--series" => args.series = value(),
            "--samples" => args.samples = value(),
            "--iters" => args.iters = value(),
            "--every" => args.every = value().max(1),
            "--poll-ms" => args.poll_ms = value() as u64,
            "--hold-secs" => args.hold_secs = value() as u64,
            "--reopen" => args.reopen = true,
            "--bench" => {}
            other => panic!("unknown flag {other}"),
        }
    }
    args
}

fn round_series(round: usize, args: &Args) -> Vec<Series> {
    let step = WINDOW_MS / args.samples.max(1) as i64;
    (0..args.series)
        .map(|i| {
            let name = if i % 2 == 0 {
                "fuzz_gauge"
            } else {
                "fuzz_requests_total"
            };
            let labels = vec![
                Label::new("fuzz_run", RUN),
                Label::new("fuzz_round", round.to_string()),
                Label::new("instance", format!("i-{}", i % 12)),
                Label::new("job", format!("job-{}", i % 3)),
                Label::new("code", ["200", "404", "500"][i % 3]),
                Label::new("series", i.to_string()),
            ];
            let samples = (0..args.samples)
                .map(|s| Sample::new(T0_MS + s as i64 * step, (round * 1000 + i + s) as f64))
                .collect();
            Series::new(name, labels, samples)
        })
        .collect()
}

fn at(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}

/// `(wal, l0)`: WAL SSTs and compacted-dir SSTs in the local store, the
/// fan-in a `DbReader` replays into immutable memtables and probes.
fn sst_counts(root: &std::path::Path) -> (usize, usize) {
    let count = |dir: &str| {
        std::fs::read_dir(root.join("growth").join(dir))
            .map(|entries| entries.count())
            .unwrap_or(0)
    };
    (count("wal"), count("compacted"))
}

fn median_ms(mut samples: Vec<Duration>) -> f64 {
    samples.sort_unstable();
    samples[samples.len() / 2].as_secs_f64() * 1e3
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = args();
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = SlateDbStorageConfig {
        path: "growth".to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: dir.path().to_string_lossy().into_owned(),
        }),
        settings_path: None,
        block_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: 64 * 1024 * 1024,
            shards: None,
        })),
        meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: 16 * 1024 * 1024,
            shards: None,
        })),
    };
    let writer = TimeSeriesDb::open(Config {
        storage: storage.clone(),
        flush_interval: Duration::from_secs(1),
        ..Config::default()
    })
    .await
    .expect("open writer");
    let open_reader = || async {
        TimeSeriesDbReader::open(
            storage.clone(),
            DbReaderOptions {
                manifest_poll_interval: Duration::from_millis(args.poll_ms),
                ..DbReaderOptions::default()
            },
            50,
        )
        .await
        .expect("open reader")
    };
    let mut reader = open_reader().await;
    let ns = Namespace::default();
    let range = at(T0_MS - HOUR_MS)..=at(T0_MS + HOUR_MS);
    let query_at = at(T0_MS + WINDOW_MS);

    println!(
        "round   pinned_ms  lv_ms  series_ms  unpinned_ms   wal  ssts  (median of {} iters)",
        args.iters
    );
    for round in 0..args.rounds {
        writer
            .write(&ns, round_series(round, &args))
            .await
            .expect("write");
        writer.flush().await.expect("flush");

        let probe = format!(r#"{{fuzz_run="{RUN}", fuzz_round="{round}"}}"#);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let seen = reader
                .series(&ns, &[probe.as_str()], range.clone())
                .await
                .expect("series");
            if seen.len() == args.series {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "round {round} never became visible"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if round % args.every != 0 && round + 1 != args.rounds {
            continue;
        }
        if args.reopen {
            reader.close().await.expect("close reader");
            reader = open_reader().await;
        }

        let pinned =
            format!(r#"sum by (job) (fuzz_gauge{{fuzz_run="{RUN}", fuzz_round="{round}"}})"#);
        let unpinned = format!(r#"count(fuzz_gauge{{fuzz_run="{RUN}", instance="i-0"}})"#);
        let (mut p, mut lv, mut s, mut u) = (vec![], vec![], vec![], vec![]);
        for _ in 0..args.iters {
            let t = Instant::now();
            reader
                .query(&ns, &pinned, Some(query_at))
                .await
                .expect("pinned");
            p.push(t.elapsed());
            let t = Instant::now();
            reader
                .label_values(&ns, "instance", Some(&[probe.as_str()]), range.clone())
                .await
                .expect("label_values");
            lv.push(t.elapsed());
            let t = Instant::now();
            reader
                .series(&ns, &[probe.as_str()], range.clone())
                .await
                .expect("series");
            s.push(t.elapsed());
            let t = Instant::now();
            reader
                .query(&ns, &unpinned, Some(query_at))
                .await
                .expect("unpinned");
            u.push(t.elapsed());
        }
        let (wal, ssts) = sst_counts(dir.path());
        println!(
            "{round:5}   {:9.3}  {:5.3}  {:9.3}  {:11.3}  {wal:4}  {ssts:4}",
            median_ms(p),
            median_ms(lv),
            median_ms(s),
            median_ms(u)
        );
    }
    if args.hold_secs > 0 {
        let unpinned = format!(r#"count(fuzz_gauge{{fuzz_run="{RUN}", instance="i-0"}})"#);
        println!("BEGIN hold pid={}", std::process::id());
        let end = Instant::now() + Duration::from_secs(args.hold_secs);
        while Instant::now() < end {
            reader
                .query(&ns, &unpinned, Some(query_at))
                .await
                .expect("unpinned");
        }
    }
    reader.close().await.expect("close reader");
    writer.close().await.expect("close writer");
}
