import { motion } from "motion/react";
import { ARCH } from "@/components/diagrams/arch-configs";
import { DEFAULT_INPUTS, estimate, usd } from "@/lib/cost-model";
import { LogsIcon, MetricsIcon, TracesIcon } from "@/components/icons";
import type { SlideDef } from "../deck/types";
import { useStep } from "../deck/context";
import { CountUp, EASE, Frame, Numeral, Reveal, Slide, Tag, useAt } from "../ui/primitives";
import { ForcesChart } from "../diagrams/ForcesChart";
import { ArchPanel, ArchStats } from "../diagrams/ArchPanel";
import { StackTiers } from "../diagrams/StackTiers";
import { LsmTree, LSM_CAPTIONS } from "../diagrams/LsmTree";
import { SlateDb, SLATE_CAPTIONS } from "../diagrams/SlateDb";
import { ReaderReplay, REPLAY_CAPTIONS } from "../diagrams/ReaderReplay";
import { EpochSharding, EPOCH_CAPTIONS } from "../diagrams/EpochSharding";
import { LatencyLadder } from "../diagrams/LatencyLadder";
import { OracleDiff } from "../diagrams/OracleDiff";
import { Czinger } from "../diagrams/Czinger";
import { SdlcLoop } from "../diagrams/SdlcLoop";
import { AlpEncoding } from "../diagrams/AlpEncoding";
import { Bm25Blocks } from "../diagrams/Bm25Blocks";
import { OPTIMIZATIONS, UPSTREAM_BUGS } from "../content/findings";

const S = {
  rethink: "01 · Observability needs to be rethought",
  sucks: "02 · Why self-hosted observability still sucks",
  object: "03 · Object-store-native databases",
  agentic: "04 · Why an agentic rebuild is viable",
  loop: "05 · The loop",
  results: "06 · Results",
  opts: "07 · Optimisations",
  bugs: "08 · Bugs we found upstream",
};

function Caption({ lines }: { lines: string[] }) {
  const { step } = useStep();
  return (
    <motion.p key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, ease: EASE }} className="max-w-[1500px] text-[26px] leading-snug text-fg">
      {lines[Math.min(step, lines.length - 1)]}
    </motion.p>
  );
}

/* ---------------------------------------------------------------- title */

function TitleSlide() {
  return (
    <div className="absolute inset-0 flex flex-col justify-center px-[120px]">
      <div className="eyebrow text-[22px]">Plural Tech Talks · 2026</div>
      <h1 className="mt-6 font-serif text-[148px] font-normal leading-[0.98] tracking-[-0.025em] text-ink">
        Rebuilding <em className="text-accent-text">LGTM</em>
        <br />
        in Rust
      </h1>
      <p className="mt-10 max-w-[1100px] text-[34px] leading-snug text-fg">
        How we rebuilt Prometheus, Loki and Tempo on object storage with agents, and checked every answer against the originals.
      </p>
      <div className="mt-16 flex items-center gap-10">
        {[
          { Icon: MetricsIcon, name: "Metrics", peer: "Prometheus · Mimir", c: "var(--metrics)" },
          { Icon: LogsIcon, name: "Logs", peer: "Loki", c: "var(--logs)" },
          { Icon: TracesIcon, name: "Traces", peer: "Tempo", c: "var(--traces)" },
        ].map(({ Icon, name, peer, c }, i) => (
          <motion.div
            key={name}
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 + i * 0.12, duration: 0.6, ease: EASE }}
            className="card relative flex items-center gap-5 px-6 py-5"
            style={{ borderTop: `3px solid ${c}` }}
          >
            <div className="grid h-14 w-14 place-items-center border border-line-strong bg-bg">
              <Icon size={34} />
            </div>
            <div>
              <div className="text-[30px] font-medium leading-tight text-ink">{name}</div>
              <div className="text-[18px] italic text-muted">{peer}-compatible</div>
            </div>
          </motion.div>
        ))}
      </div>
    </div>
  );
}

function Agenda() {
  const items = Object.values(S).map((s) => s.split(" · ")[1]);
  return (
    <Slide eyebrow="Agenda" title="What we'll cover">
      <div className="grid grid-cols-2 gap-x-20 gap-y-1">
        {items.map((t, i) => (
          <Reveal key={t} delay={i * 0.06} className="flex items-baseline gap-6 border-b border-line py-5">
            <Numeral n={i + 1} />
            <span className="text-[34px] text-ink">{t}</span>
          </Reveal>
        ))}
      </div>
    </Slide>
  );
}

/* ------------------------------------------------------ 01 rethink */

const FORCES = [
  { t: "Agents are accelerating software creation", s: "More services, more deploys, more code nobody has read." },
  { t: "All of that software needs to be observed", s: "Telemetry grows with the code, not with the team." },
  { t: "Usage-based vendor pricing is extractive", s: "The bill tracks volume, so it grows fastest of all." },
  { t: "Self-hosted observability is massively unwieldy", s: "Its operating cost grows in step with volume: more pods, more rings, more people." },
];

function Forces() {
  const { step } = useStep();
  return (
    <Slide eyebrow={S.rethink} title="Four things that can't all stay true">
      <div className="grid h-full grid-cols-[1fr_820px] gap-12">
        <div className="flex flex-col gap-5">
          {FORCES.map((f, i) => (
            <Reveal key={f.t} at={i} className="flex gap-5">
              <Numeral n={i + 1} />
              <div>
                <div className="text-[32px] leading-tight text-ink">{f.t}</div>
                <div className="mt-1 text-[22px] text-muted">{f.s}</div>
              </div>
            </Reveal>
          ))}
          <Reveal at={4} className="mt-4 font-serif text-[46px] italic text-danger">
            Something has to give.
          </Reveal>
        </div>
        <div className="relative">
          <ForcesChart />
          <span className="sr-only">{step}</span>
        </div>
      </div>
    </Slide>
  );
}

/* ------------------------------------------------------ 02 sucks */

const LOKI_LEVELS = [0, 2, 4, 5];

function LokiAtScale() {
  const { step } = useStep();
  const level = LOKI_LEVELS[Math.min(step, 3)];
  const cfg = ARCH.logs;
  return (
    <Slide eyebrow={S.sucks} title="Loki at scale">
      <div className="absolute inset-0 -top-6 grid grid-cols-[700px_1fr] gap-12">
        <Frame className="self-start">
          <ArchPanel side={cfg.peer} mode="both" level={level} color="var(--faint)" showStats={false} />
        </Frame>
        <div className="flex flex-col gap-5 text-[26px] leading-snug text-fg">
          <div className="flex gap-3">
            {LOKI_LEVELS.map((l, i) => (
              <span key={l} className={`code rounded-[3px] border px-3 py-1 text-[19px] transition-colors ${i === Math.min(step, 3) ? "border-accent bg-accent text-white" : "border-line-strong text-muted"}`}>
                {cfg.levels[l]}
              </span>
            ))}
          </div>
          <ArchStats side={cfg.peer} level={level} className="card -mx-px" />
          <p>Nine kinds of component, each with its own ring membership, cache sizing and failure modes.</p>
          <Reveal at={1}>
            <p>Every write fans out to three ingesters, each with a WAL on a persistent volume. Two of the three sit in other zones, so you pay cross-AZ traffic on every byte.</p>
          </Reveal>
          <Reveal at={2}>
            <p>Scaling ingesters means rebalancing the hash ring. Scaling down means flushing in-memory chunks one pod at a time.</p>
          </Reveal>
          <Reveal at={3}>
            <p className="font-serif text-[36px] italic text-ink">None of these are bad decisions. They follow from keeping recent data in replicated memory.</p>
          </Reveal>
        </div>
      </div>
    </Slide>
  );
}

function Tiers() {
  return (
    <Slide eyebrow={S.sucks} title="Every stack changes shape as it grows">
      <div className="grid h-full grid-rows-[1fr_auto] gap-6">
        <StackTiers />
        <div className="flex items-end justify-between gap-10 border-t border-line pt-5">
          <blockquote className="max-w-[1150px] font-serif text-[30px] italic leading-snug text-ink">
            “Elasticsearch mutates. LGTM mutates. Datadog stays operationally simple but mutates financially…”
          </blockquote>
          <cite className="code shrink-0 text-right text-[16px] not-italic text-muted">
            Mat Duggan, “Clickhouse is winning
            <br />
            the Observability Wars”, 2026
          </cite>
        </div>
      </div>
    </Slide>
  );
}

const RACE = [
  { name: "Grafana Tempo", ours: "Plural Traces", ratio: 0.2, label: "5× faster median trace query", color: "var(--traces)" },
  { name: "Grafana Loki", ours: "Plural Logs", ratio: 0.63, label: "0.63× median per-query latency", color: "var(--logs)" },
];

function Slow() {
  const { step, live } = useStep();
  const go = step >= 1;
  return (
    <Slide
      eyebrow={S.sucks}
      title="…and LGTM is painfully slow"
      lede="LGTM has a lot of what we'd want: object storage for operational simplicity, and it's self-hostable. But it's painfully slow in practice. Rust can speed that up."
    >
      <div className="flex flex-col gap-10">
        {RACE.map((r) => (
          <div key={r.name} className="grid grid-cols-[300px_1fr] items-center gap-y-3">
            {[
              { n: r.name, t: 1, c: "var(--faint)" },
              { n: r.ours, t: r.ratio, c: r.color },
            ].map((row) => (
              <div key={row.n} className="contents">
                <div className="text-[26px] text-ink">{row.n}</div>
                <div className="relative h-[34px] bg-bg-subtle">
                  <motion.div
                    className="absolute inset-y-0 left-0"
                    style={{ background: row.c }}
                    initial={live ? { width: 0 } : false}
                    animate={{ width: go ? `${row.t * 100}%` : 0 }}
                    transition={{ duration: go ? 2.4 * row.t : 0.2, ease: "linear" }}
                  />
                </div>
              </div>
            ))}
            <div />
            <Reveal at={1} delay={2.4} className="code text-[20px] text-muted">
              {r.label}
            </Reveal>
          </div>
        ))}
        <div className="code text-[16px] text-faint">Same data, same queries, same host. Median of published differential runs (telemetry.plural.sh, Benchmarks).</div>
      </div>
    </Slide>
  );
}

/* ------------------------------------------------------ 03 object store */

function WriteHeavy() {
  const rows = [
    ["Append-only, never updated", "Immutable files: perfect for object storage"],
    ["Arrives roughly in time order", "Files carry time bounds, so queries prune whole files"],
    ["Written constantly, read in bursts", "Batch writes into big PUTs, cache what's read"],
    ["Read as scans and aggregations", "Columnar blocks, compressed 8–14×"],
    ["Durability matters more than latency", "S3's 11 nines replace three replicas"],
  ];
  return (
    <Slide eyebrow={S.object} title="Observability is the ideal object-store workload" lede="Write-heavy, read-light, and analytical. Every property of the data maps to something an LSM tree on S3 does well.">
      <div className="grid grid-cols-[1fr_80px_1fr] gap-y-0">
        <div className="label pb-3 text-[15px]">telemetry is…</div>
        <div />
        <div className="label pb-3 text-[15px]">so an object-store LSM gives you…</div>
        {rows.map(([a, b], i) => (
          <div key={a} className="contents">
            <Reveal at={0} delay={i * 0.12} className="border-t border-line py-4 text-[28px] text-ink">
              {a}
            </Reveal>
            <Reveal at={0} delay={i * 0.12 + 0.3} className="grid place-items-center border-t border-line text-[28px] text-accent-text">
              →
            </Reveal>
            <Reveal at={0} delay={i * 0.12 + 0.4} className="border-t border-line py-4 text-[28px] text-fg">
              {b}
            </Reveal>
          </div>
        ))}
      </div>
    </Slide>
  );
}

function Lsm() {
  return (
    <Slide eyebrow={S.object} title="LSM trees in one picture">
      <div className="flex h-full flex-col justify-between gap-6">
        <LsmTree />
        <Caption lines={LSM_CAPTIONS} />
      </div>
    </Slide>
  );
}

function Slate() {
  return (
    <Slide eyebrow={S.object} title={<>SlateDB: an LSM tree with <em>no local disk</em></>}>
      <div className="flex h-full flex-col justify-between gap-6 pt-6">
        <SlateDb />
        <Caption lines={SLATE_CAPTIONS} />
      </div>
    </Slide>
  );
}

function Replay() {
  return (
    <Slide eyebrow={S.object} title="One writer, many readers, replay from the WAL">
      <div className="flex h-full flex-col justify-between gap-6">
        <ReaderReplay />
        <Caption lines={REPLAY_CAPTIONS} />
      </div>
    </Slide>
  );
}

function Epochs() {
  return (
    <Slide eyebrow={S.object} title="Many writers: epoch sharding">
      <div className="flex h-full flex-col justify-between gap-6">
        <EpochSharding />
        <Caption lines={EPOCH_CAPTIONS} />
      </div>
    </Slide>
  );
}

function Friction() {
  return (
    <Slide eyebrow={S.object} title="The friction: slow GETs, per-request PUTs">
      <LatencyLadder />
    </Slide>
  );
}

/* ------------------------------------------------------ 04 agentic */

function Enablers() {
  const cards = [
    { n: 1, t: "Rust", s: "Memory safety without a GC, and a type system that works like a proof checker. Agents get a strict reviewer that answers in seconds.", c: "var(--traces)" },
    { n: 2, t: "An oracle", s: "Prometheus, Loki and Tempo already exist. Every answer can be diffed live against a working implementation.", c: "var(--logs)" },
    { n: 3, t: "Tireless optimisation", s: "Hundreds of micro-optimisations that no human team would budget for become worth doing.", c: "var(--accent-text)" },
  ];
  return (
    <Slide eyebrow={S.agentic} title="Why rebuilding with agents actually works" lede="Rewrites usually fail on correctness and on the long tail of behaviour. Three things change that here.">
      <div className="grid h-full grid-cols-3 gap-8">
        {cards.map((c, i) => (
          <Reveal key={c.t} at={i} className="h-full">
            <Frame className="h-full" bodyClassName="flex h-full flex-col p-8">
              <div className="absolute inset-x-0 top-0 h-[4px]" style={{ background: c.c }} />
              <Numeral n={c.n} color={c.c} />
              <div className="mt-6 font-serif text-[54px] leading-none text-ink">{c.t}</div>
              <p className="mt-6 text-[26px] leading-snug text-fg">{c.s}</p>
            </Frame>
          </Reveal>
        ))}
      </div>
    </Slide>
  );
}

const WARM_BAD = [
  { t: "async fn warm(&self, ids: Vec<SstId>) -> Result<()> {" },
  { t: "    let mut cache = self.cache.lock().unwrap();", mark: true },
  { t: "    for id in ids {" },
  { t: "        let block = self.store.get(id).await?;  // S3 GET", mark: true },
  { t: "        cache.insert(id, block);" },
  { t: "    }" },
  { t: "    Ok(())" },
  { t: "}" },
  { t: "" },
  { t: "tokio::spawn(db.warm(ids));" },
];

const WARM_GOOD = [
  { t: "async fn warm(&self, ids: Vec<SstId>) -> Result<()> {" },
  { t: "    let blocks = try_join_all(ids.iter().map(|id| self.store.get(*id)))", mark: true },
  { t: "        .await?;", mark: true },
  { t: "    let mut cache = self.cache.lock().unwrap();", mark: true },
  { t: "    cache.extend(ids.into_iter().zip(blocks));", mark: true },
  { t: "    Ok(())" },
  { t: "}" },
  { t: "" },
  { t: "tokio::spawn(db.warm(ids));" },
];

const RUSTC_ERR = `error: future cannot be sent between threads safely
  |
  |  let mut cache = self.cache.lock().unwrap();
  |      --------- has type \`MutexGuard<'_, BlockCache>\`, which is not \`Send\`
  |  let block = self.store.get(id).await?;
  |                                ^^^^^ await occurs here, with \`cache\` maybe used later`;

function RustSlide() {
  const err = useAt(1);
  const ok = useAt(2);
  const code = ok ? WARM_GOOD : WARM_BAD;
  const tint = ok ? "var(--ok)" : "var(--danger)";
  return (
    <Slide eyebrow={S.agentic} title="Rust: the compiler as a proof checker">
      <div className="grid h-full grid-cols-[minmax(0,1fr)_540px] gap-12">
        <div className="flex flex-col gap-5">
          <Frame label="block cache warming" right="simplified">
            <motion.pre key={ok ? "good" : "bad"} initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.4 }} className="code px-6 py-5 text-[22px] leading-[1.55] text-ink">
              {code.map((l, i) => (
                <div key={i} className="-mx-6 px-6" style={l.mark && err ? { background: `color-mix(in srgb, ${tint} 10%, transparent)` } : undefined}>
                  {l.t || " "}
                </div>
              ))}
            </motion.pre>
          </Frame>
          <div className="relative min-h-[190px]">
            <Reveal at={1} until={1} className="absolute inset-0">
              <pre className="code border-l-2 border-danger pl-5 text-[17px] leading-snug text-danger">{RUSTC_ERR}</pre>
            </Reveal>
            <Reveal at={2} className="absolute inset-0">
              <pre className="code border-l-2 border-ok pl-5 text-[19px] leading-snug text-ok">{"$ cargo check\n    Finished `dev` profile"}</pre>
            </Reveal>
          </div>
        </div>
        <div className="flex flex-col gap-7 text-[27px] leading-snug text-fg">
          <p>
            <span className="text-ink">The compiler tracks ownership across threads.</span> It knows which task holds what, and what is safe to send between them.
          </p>
          <Reveal at={1}>
            <p>
              <span className="text-ink">Concurrency bugs become compile errors.</span> Holding a lock across an S3 GET compiles in Go or Java, then stalls every reader for 50 ms
              under load. Here it never builds.
            </p>
          </Reveal>
          <Reveal at={2}>
            <p>
              <span className="text-ink">A fast, strict reviewer.</span> The fix fetches concurrently and locks only to insert. Agents iterate against the compiler in seconds, so
              whole classes of bugs never reach the fuzzer.
            </p>
          </Reveal>
        </div>
      </div>
    </Slide>
  );
}

function Oracle() {
  return (
    <Slide eyebrow={S.agentic} title="The oracle: diff against the real thing">
      <OracleDiff />
    </Slide>
  );
}

function CzingerSlide() {
  return (
    <Slide eyebrow={S.agentic} title="Hundreds of micro-optimisations">
      <Czinger />
    </Slide>
  );
}

/* ------------------------------------------------------ 05 loop */

function Loop() {
  return (
    <Slide eyebrow={S.loop} title="The loop we run">
      <SdlcLoop />
    </Slide>
  );
}

/* ------------------------------------------------------ 06 results */

function Bench() {
  const stats = [
    { v: 5, d: 0, p: "", s: "×", label: "faster median trace query than Tempo", c: "var(--traces)", Icon: TracesIcon },
    { v: 0.63, d: 2, p: "", s: "×", label: "median per-query latency of Loki", c: "var(--logs)", Icon: LogsIcon },
    { v: 18, d: 0, p: "1/", s: "", label: "of Mimir's CPU for the same workload", c: "var(--metrics)", Icon: MetricsIcon },
  ];
  return (
    <Slide eyebrow={S.results} title="Benchmarks" lede="Every product is fuzzed against its peer on identical data. Latency, CPU and memory for both sides are published, including the runs we lose.">
      <div className="grid h-full grid-cols-3 gap-8">
        {stats.map((s, i) => (
          <Reveal key={s.label} delay={i * 0.15} className="h-full">
            <Frame className="h-full" bodyClassName="flex h-full flex-col justify-between p-8">
              <div className="absolute inset-x-0 top-0 h-[4px]" style={{ background: s.c }} />
              <s.Icon size={40} />
              <div>
                <div className="font-serif text-[150px] leading-none text-ink">
                  <CountUp to={s.v} decimals={s.d} prefix={s.p} suffix={s.s} />
                </div>
                <div className="mt-4 text-[26px] leading-snug text-fg">{s.label}</div>
              </div>
            </Frame>
          </Reveal>
        ))}
      </div>
    </Slide>
  );
}

function ArchCompare() {
  const { step } = useStep();
  const mode = step === 0 ? "write" : step === 1 ? "read" : "both";
  const cfg = ARCH.logs;
  return (
    <Slide eyebrow={S.results} title="Same job, far less machinery">
      <div className="flex h-full flex-col gap-5">
        <div className="flex items-center gap-3">
          {["write path", "read path", "both"].map((m, i) => (
            <span key={m} className={`code rounded-[3px] border px-3 py-1 text-[19px] ${i === Math.min(step, 2) ? "border-accent bg-accent text-white" : "border-line-strong text-muted"}`}>
              {m}
            </span>
          ))}
          <span className="eyebrow ml-auto text-[17px]">5 TB/day of logs · pod counts are illustrative sizing, not benchmarks</span>
        </div>
        <div className="grid grid-cols-[580px_580px_1fr] gap-8">
          <Frame>
            <ArchPanel side={cfg.ours} mode={mode} level={4} color="var(--logs)" showStats={false} />
          </Frame>
          <Frame>
            <ArchPanel side={cfg.peer} mode={mode} level={4} color="var(--faint)" showStats={false} />
          </Frame>
          <div className="flex flex-col justify-center">
            <div className="grid grid-cols-[1fr_auto_auto] items-baseline gap-x-6 gap-y-5">
              <span />
              <span className="label text-right text-[14px] text-logs">Plural</span>
              <span className="label text-right text-[14px]">Loki</span>
              {cfg.ours.stats(4, 2).map((s, i) => (
                <div key={s.label} className="contents">
                  <span className="border-t border-line pt-3 text-[22px] text-fg">{s.label}</span>
                  <span className="border-t border-line pt-3 text-right font-serif text-[44px] leading-none text-ink">{s.value}</span>
                  <span className="border-t border-line pt-3 text-right font-serif text-[44px] leading-none text-muted">{cfg.peer.stats(4, 2)[i].value}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </Slide>
  );
}

const YAML = `apiVersion: telemetry.plural.sh/v1alpha1
kind: Logs
metadata:
  name: logs
spec:
  mode: Sharded
  config:
    retention: 14d
    storage:
      objectStore:
        type: Aws
        aws: { region: us-east-1, bucket: acme-telemetry }
  writer:
    replicas: 3
  reader:
    replicas: 2`;

function Operator() {
  const { step, live } = useStep();
  const pods = step >= 1;
  return (
    <Slide eyebrow={S.results} title="Three YAML files and a bucket" lede="A Kubernetes operator owns the lifecycle: StatefulSets for identity, a ShardMap custom resource for routing, Leases for ownership.">
      <div className="grid h-full grid-cols-[860px_1fr] gap-12">
        <Frame label="logs.yaml" right="kubectl apply -f">
          <pre className="code px-6 py-5 text-[21px] leading-[1.5] text-ink">
            {YAML.split("\n").map((l, i) => (
              <motion.div key={i} initial={live ? { opacity: 0 } : false} animate={{ opacity: 1 }} transition={{ delay: i * 0.05 }}>
                <span className={/^\s*\w+:/.test(l) ? "" : ""}>
                  <span className="text-muted">{l.match(/^\s*[\w]+:/)?.[0] ?? ""}</span>
                  {l.replace(/^\s*[\w]+:/, "")}
                </span>
              </motion.div>
            ))}
          </pre>
        </Frame>
        <div className="flex flex-col gap-6">
          <Reveal at={1}>
            <pre className="code rounded-[3px] bg-ink px-5 py-4 text-[19px] leading-relaxed text-bg">
              {"$ kubectl -n telemetry get logs\nNAME   MODE      READY   AGE\nlogs   Sharded   True    2m"}
            </pre>
          </Reveal>
          <div className="grid grid-cols-2 gap-6">
            {[
              { name: "writer", n: 3, c: "var(--logs)" },
              { name: "reader", n: 6, c: "var(--accent-text)" },
            ].map((g) => (
              <Frame key={g.name} label={`${g.name} StatefulSet`} right={pods ? `×${g.n}` : "×0"} bodyClassName="flex flex-wrap gap-3 p-5 min-h-[120px]">
                {Array.from({ length: g.n }, (_, i) => (
                  <motion.div
                    key={i}
                    className="grid h-[56px] w-[56px] place-items-center rounded-[3px] border"
                    style={{ borderColor: g.c, background: `color-mix(in srgb, ${g.c} 12%, var(--panel))` }}
                    initial={false}
                    animate={{ scale: pods ? 1 : 0, opacity: pods ? 1 : 0 }}
                    transition={{ delay: pods ? 0.3 + i * 0.12 : 0, type: "spring", stiffness: 300, damping: 20 }}
                  >
                    <span className="code text-[15px] text-ink">{i}</span>
                  </motion.div>
                ))}
              </Frame>
            ))}
          </div>
          <Reveal at={2}>
            <p className="text-[27px] leading-snug text-fg">
              No ZooKeeper, no etcd, no memberlist, no memcached and no persistent volumes to babysit. Upgrade or roll back with <span className="text-ink">one custom resource</span>.
            </p>
          </Reveal>
        </div>
      </div>
    </Slide>
  );
}

function Cost() {
  const { step, live } = useStep();
  const results = estimate(DEFAULT_INPUTS);
  const max = Math.max(...results.map((r) => r.total));
  const scale = (v: number) => Math.log10(Math.max(v, 1)) / Math.log10(max);
  const colors = ["var(--accent-text)", "var(--faint)", "var(--warning)"];
  return (
    <Slide eyebrow={S.results} title="What 5 TB/day of logs costs" lede="15-day retention, AWS us-east-1 on-demand, Datadog list price. These are the cost model from the docs site, not quotes.">
      <div className="flex flex-col gap-8">
        {results.map((r, i) => (
          <div key={r.name} className="grid grid-cols-[320px_1fr_220px] items-center gap-6">
            <div className="text-[30px] text-ink">{r.name}</div>
            <div className="relative h-[52px] bg-bg-subtle">
              <motion.div
                className="absolute inset-y-0 left-0"
                style={{ background: colors[i] }}
                initial={live ? { width: 0 } : false}
                animate={{ width: `${scale(r.total) * 100}%` }}
                transition={{ duration: 1.1, ease: EASE, delay: 0.2 + i * 0.2 }}
              />
            </div>
            <div className="code text-right text-[30px] text-ink">{usd(r.total)}/mo</div>
          </div>
        ))}
        <div className="code text-[16px] text-faint">log scale</div>
        <Reveal at={1} className="grid grid-cols-3 gap-8">
          {[
            ["No RF=3", "Each record is written once, to one shard owner. S3 provides durability."],
            ["No cross-AZ replication", "Loki pays about $2,000/month at this volume just to copy bytes between zones."],
            ["No component sprawl", "Two StatefulSets instead of nine services and three memcached fleets."],
          ].map(([t, s]) => (
            <div key={t} className="border-t-2 border-accent pt-4">
              <div className="text-[28px] text-ink">{t}</div>
              <div className="mt-2 text-[22px] leading-snug text-fg">{s}</div>
            </div>
          ))}
        </Reveal>
        <span className="sr-only">{step}</span>
      </div>
    </Slide>
  );
}

/* ------------------------------------------------------ 07 optimisations */

function Alp() {
  return (
    <Slide eyebrow={S.opts} title="ALP: floats that compress like integers">
      <AlpEncoding />
    </Slide>
  );
}

function Bm25() {
  return (
    <Slide eyebrow={S.opts} title="BM25 search that skips most of the index">
      <Bm25Blocks />
    </Slide>
  );
}

function MoreOpts() {
  return (
    <Slide eyebrow={S.opts} title="…and a long tail of smaller wins">
      <div className="grid grid-cols-4 gap-6">
        {OPTIMIZATIONS.map((o, i) => (
          <Reveal key={o.title} delay={i * 0.07} className="h-full">
            <div className="card flex h-full flex-col gap-3 p-6">
              <div className="flex items-center justify-between gap-3">
                <Tag>{o.tag}</Tag>
                {o.stat && <span className="code text-[20px] text-accent-text">{o.stat}</span>}
              </div>
              <div className="text-[26px] leading-tight text-ink">{o.title}</div>
              <div className="text-[19px] leading-snug text-fg">{o.body}</div>
            </div>
          </Reveal>
        ))}
      </div>
    </Slide>
  );
}

/* ------------------------------------------------------ 08 bugs */

function Bugs() {
  return (
    <Slide eyebrow={S.bugs} title="The oracle is wrong too, sometimes" lede="A differential fuzzer doesn't know which side is right. Some mismatches turned out to be bugs or quirks in the systems we were copying.">
      <div className="grid grid-cols-3 gap-8">
        {UPSTREAM_BUGS.map((b, i) => (
          <Reveal key={b.title} at={0} delay={i * 0.15} className="h-full">
            <Frame className="h-full" bodyClassName="flex h-full flex-col gap-4 p-7">
              <Tag color="var(--danger)">{b.tag}</Tag>
              <div className="font-serif text-[40px] leading-tight text-ink">{b.title}</div>
              <p className="text-[22px] leading-snug text-fg">{b.body}</p>
              {b.stat && <div className="code mt-auto text-[18px] text-muted">{b.stat}</div>}
            </Frame>
          </Reveal>
        ))}
      </div>
    </Slide>
  );
}

/* ------------------------------------------------------ close */

function Closing() {
  return (
    <div className="absolute inset-0 flex flex-col justify-center px-[120px]">
      <div className="eyebrow text-[22px]">Plural Telemetry</div>
      <h1 className="mt-6 font-serif text-[132px] font-normal leading-[1] tracking-[-0.025em] text-ink">
        Observability
        <br />
        should be <em className="text-accent-text">cheap</em>.
      </h1>
      <div className="mt-16 grid max-w-[1400px] grid-cols-3 gap-10 text-[26px]">
        {[
          ["Docs", "telemetry.plural.sh"],
          ["Code", "github.com/pluralsh/telemetry"],
          ["License", "AGPL v3 · operator Apache 2.0"],
        ].map(([k, v]) => (
          <div key={k} className="border-t border-line-strong pt-4">
            <div className="label text-[15px]">{k}</div>
            <div className="code mt-2 text-ink">{v}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

export const SLIDES: SlideDef[] = [
  { id: "title", section: "Plural Tech Talks", title: "Rebuilding LGTM in Rust", Component: TitleSlide, notes: "Intro. What Plural Telemetry is: Prometheus/Mimir, Loki and Tempo compatible databases, written in Rust on SlateDB." },
  { id: "agenda", section: "Agenda", title: "Agenda", Component: Agenda },
  {
    id: "forces",
    section: S.rethink,
    title: "Four things that can't all stay true",
    steps: 4,
    Component: Forces,
    notes: "Walk the four curves. Agents ship more code, which means more telemetry. Vendor bills grow fastest, and the cost of operating a self-hosted stack grows in proportion to volume, so neither option escapes the curve.",
  },
  {
    id: "loki",
    section: S.sucks,
    title: "Loki at scale",
    steps: 3,
    Component: LokiAtScale,
    notes: "Each step raises ingest: 100 GB → 1 TB → 5 TB → 10 TB/day. Watch the pod squares multiply across nine component kinds. Diagram is the docs site's architecture page.",
  },
  {
    id: "tiers",
    section: S.sucks,
    title: "Every stack changes shape",
    steps: 2,
    Component: Tiers,
    notes: "Cost ranges from Mat Duggan's article (ClickHouse left out on purpose). Step through 1 / 5 / 10 TB per day. LGTM goes from a handful of pods to 65+ to 180+ and a dedicated team.",
  },
  { id: "slow", section: S.sucks, title: "…and it's slow", steps: 1, Component: Slow, notes: "UX matters. Step 1 races the published median latency ratios." },
  { id: "write-heavy", section: S.object, title: "The ideal object-store workload", Component: WriteHeavy },
  { id: "friction", section: S.object, title: "Object-store friction", steps: 3, Component: Friction, notes: "GET latency is ~250,000× RAM. Fix with Foyer RAM+NVMe, cache warming, request dedupe. PUT cost fixed by group commit." },
  { id: "lsm", section: S.object, title: "LSM trees", steps: 5, Component: Lsm, notes: "Memtable → flush to L0 → more flushes → read amplification with get(23) → compaction → cheap read." },
  { id: "slatedb", section: S.object, title: "SlateDB", steps: 3, Component: Slate, notes: "WAL objects every 100 ms; L0 flush + manifest; compactor; reader with Foyer cache." },
  { id: "replay", section: S.object, title: "Reader/writer split", steps: 3, Component: Replay, notes: "Readers poll the manifest, replay WAL newer than replay_after_wal. Final step: writer fencing via epoch CAS." },
  { id: "epochs", section: S.object, title: "Epoch sharding", steps: 2, Component: Epochs, notes: "SlateDB is single-writer; we add many writers by sharding. Scale-up changes the future only." },
  { id: "enablers", section: S.agentic, title: "Why agents work here", steps: 2, Component: Enablers },
  { id: "rust", section: S.agentic, title: "Rust", steps: 2, Component: RustSlide, notes: "An agent holds a std Mutex guard across an S3 await. tokio::spawn needs a Send future and MutexGuard isn't Send, so it fails to compile. In Go this ships and serialises every reader behind a network call. Step 2: fetch concurrently, lock only to insert." },
  { id: "oracle", section: S.agentic, title: "The oracle", steps: 2, Component: Oracle, notes: "Seeded fuzzer, identical data and queries to both. Mismatches are replayable and become regression fixtures." },
  { id: "czinger", section: S.agentic, title: "Czinger 21C", Component: CzingerSlide, notes: "The 21C chassis is generatively designed and 3D printed: organic, bone-like, not something a human would draw. Agents optimise code the same way, through hundreds of small wins, and the oracle is what makes the strange shape safe to ship." },
  { id: "loop", section: S.loop, title: "The loop", steps: 4, Component: Loop, notes: "Implement + unit test → differential fuzz → benchmark (≥2×) → repeat." },
  { id: "bench", section: S.results, title: "Benchmarks", Component: Bench },
  { id: "arch", section: S.results, title: "Architecture", steps: 2, Component: ArchCompare, notes: "Write path, read path, both. Two StatefulSets vs nine components, zero replicated disks, zero hash rings." },
  { id: "operator", section: S.results, title: "Operator setup", steps: 2, Component: Operator },
  { id: "cost", section: S.results, title: "Cost", steps: 1, Component: Cost },
  { id: "alp", section: S.opts, title: "ALP encoding", steps: 3, Component: Alp },
  { id: "bm25", section: S.opts, title: "BM25 block-max search", steps: 2, Component: Bm25 },
  { id: "more-opts", section: S.opts, title: "More optimisations", Component: MoreOpts },
  { id: "bugs", section: S.bugs, title: "Upstream bugs", Component: Bugs },
  { id: "closing", section: "Thanks", title: "Observability should be cheap", Component: Closing },
];
