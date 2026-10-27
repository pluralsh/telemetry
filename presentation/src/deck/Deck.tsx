import { Component, useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { AnimatePresence, motion } from "motion/react";
import { StepContext } from "./context";
import { Stage } from "./Stage";
import { Chrome } from "./Chrome";
import type { Position, SlideDef } from "./types";

const CHANNEL = "telemetry-talk";

function parseHash(slides: SlideDef[]): Position {
  const [, s, k] = window.location.hash.match(/^#\/(\d+)(?:\/(\d+))?/) ?? [];
  const slide = Math.min(Math.max(Number(s ?? 1) - 1, 0), slides.length - 1);
  const step = Math.min(Math.max(Number(k ?? 0), 0), slides[slide].steps ?? 0);
  return { slide, step };
}

class SlideBoundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    if (!this.state.error) return this.props.children;
    return <pre className="code m-16 whitespace-pre-wrap text-[22px] text-[var(--danger)]">{String(this.state.error.stack ?? this.state.error)}</pre>;
  }
}

export function SlideView({ def, step, live, index, total }: { def: SlideDef; step: number; live: boolean; index: number; total: number }) {
  const ctx = useMemo(() => ({ step, steps: def.steps ?? 0, live }), [step, def.steps, live]);
  return (
    <StepContext.Provider value={ctx}>
      <Chrome section={def.section} index={index} total={total}>
        <SlideBoundary key={def.id}>
          <def.Component />
        </SlideBoundary>
      </Chrome>
    </StepContext.Provider>
  );
}

export function Deck({ slides }: { slides: SlideDef[] }) {
  const presenter = new URLSearchParams(window.location.search).has("presenter");
  const [pos, setPos] = useState<Position>(() => parseHash(slides));
  const [dir, setDir] = useState(1);
  const [overview, setOverview] = useState(false);
  const [blank, setBlank] = useState(false);
  const [dark, setDark] = useState(() => localStorage.getItem("talk-theme") === "dark");
  const channel = useRef<BroadcastChannel | null>(null);
  const digits = useRef("");

  const go = useCallback(
    (next: Position) => {
      setPos((cur) => {
        if (cur.slide === next.slide && cur.step === next.step) return cur;
        setDir(next.slide > cur.slide || (next.slide === cur.slide && next.step > cur.step) ? 1 : -1);
        return next;
      });
    },
    [],
  );

  const step = useCallback(
    (delta: 1 | -1, skipSteps = false) => {
      setPos((cur) => {
        const steps = slides[cur.slide].steps ?? 0;
        let next: Position;
        if (delta > 0) {
          if (!skipSteps && cur.step < steps) next = { slide: cur.slide, step: cur.step + 1 };
          else if (cur.slide < slides.length - 1) next = { slide: cur.slide + 1, step: 0 };
          else return cur;
        } else {
          if (!skipSteps && cur.step > 0) next = { slide: cur.slide, step: cur.step - 1 };
          else if (cur.slide > 0) {
            const prev = cur.slide - 1;
            next = { slide: prev, step: skipSteps ? 0 : (slides[prev].steps ?? 0) };
          } else return cur;
        }
        setDir(delta);
        return next;
      });
    },
    [slides],
  );

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("talk-theme", dark ? "dark" : "light");
  }, [dark]);

  useEffect(() => {
    const hash = `#/${pos.slide + 1}${pos.step ? `/${pos.step}` : ""}`;
    if (window.location.hash !== hash) history.replaceState(null, "", hash);
    document.title = `${pos.slide + 1}. ${slides[pos.slide].title} · Rebuilding LGTM in Rust`;
    channel.current?.postMessage({ type: "pos", pos });
  }, [pos, slides]);

  useEffect(() => {
    const ch = new BroadcastChannel(CHANNEL);
    channel.current = ch;
    ch.onmessage = (e) => {
      if (e.data?.type === "pos") go(e.data.pos);
      else if (e.data?.type === "theme") setDark(e.data.dark);
    };
    const onHash = () => go(parseHash(slides));
    window.addEventListener("hashchange", onHash);
    return () => {
      ch.close();
      window.removeEventListener("hashchange", onHash);
    };
  }, [go, slides]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      if ((e.target as HTMLElement)?.closest("input, textarea, select")) return;
      const k = e.key;
      if (/^\d$/.test(k)) {
        digits.current += k;
        return;
      }
      if (k === "Enter" && digits.current) {
        const n = Math.min(Math.max(Number(digits.current), 1), slides.length) - 1;
        digits.current = "";
        go({ slide: n, step: 0 });
        setOverview(false);
        return;
      }
      digits.current = "";
      if (["ArrowRight", "ArrowDown", "PageDown", " ", "Enter"].includes(k)) step(1, e.shiftKey);
      else if (["ArrowLeft", "ArrowUp", "PageUp", "Backspace"].includes(k)) step(-1, e.shiftKey);
      else if (k === "Home") go({ slide: 0, step: 0 });
      else if (k === "End") go({ slide: slides.length - 1, step: 0 });
      else if (k === "o" || (k === "Escape" && overview)) setOverview((v) => !v);
      else if (k === "b" || k === ".") setBlank((v) => !v);
      else if (k === "d") {
        setDark((v) => {
          channel.current?.postMessage({ type: "theme", dark: !v });
          return !v;
        });
      } else if (k === "f") {
        if (document.fullscreenElement) document.exitFullscreen();
        else document.documentElement.requestFullscreen();
      } else if (k === "p" && !presenter) {
        window.open(`${window.location.pathname}?presenter${window.location.hash}`, "telemetry-presenter", "width=1400,height=860");
      } else return;
      e.preventDefault();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [go, step, slides.length, overview, presenter]);

  if (presenter) {
    return <PresenterView slides={slides} pos={pos} onStep={step} />;
  }

  const def = slides[pos.slide];

  return (
    <div className="fixed inset-0 bg-[var(--letterbox)]">
      <Stage>
        <AnimatePresence initial={false} custom={dir} mode="popLayout">
          <motion.div
            key={def.id}
            custom={dir}
            className="absolute inset-0"
            variants={{
              enter: (d: number) => ({ opacity: 0, x: d * 60 }),
              center: { opacity: 1, x: 0 },
              exit: (d: number) => ({ opacity: 0, x: d * -60 }),
            }}
            initial="enter"
            animate="center"
            exit="exit"
            transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
          >
            <SlideView def={def} step={pos.step} live index={pos.slide} total={slides.length} />
          </motion.div>
        </AnimatePresence>
        <div className="pointer-events-none absolute inset-0 z-50 bg-black transition-opacity duration-300" style={{ opacity: blank ? 1 : 0 }} />
      </Stage>

      <AnimatePresence>
        {overview && (
          <Overview
            slides={slides}
            current={pos.slide}
            onPick={(i) => {
              go({ slide: i, step: 0 });
              setOverview(false);
            }}
          />
        )}
      </AnimatePresence>

      <NavHint />
    </div>
  );
}

function Overview({ slides, current, onPick }: { slides: SlideDef[]; current: number; onPick: (i: number) => void }) {
  const ref = useRef<HTMLButtonElement>(null);
  useEffect(() => ref.current?.scrollIntoView({ block: "center" }), []);
  return (
    <motion.div
      className="thin-scroll fixed inset-0 z-[60] overflow-y-auto bg-[var(--bg)]/95 p-10 backdrop-blur"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.2 }}
    >
      <div className="mx-auto grid max-w-[1800px] grid-cols-4 gap-6">
        {slides.map((s, i) => (
          <button
            key={s.id}
            ref={i === current ? ref : undefined}
            onClick={() => onPick(i)}
            className={`group text-left`}
          >
            <div
              className={`aspect-video overflow-hidden border bg-[var(--bg)] transition-colors ${i === current ? "border-[var(--accent)]" : "border-[var(--line-strong)] group-hover:border-[var(--accent-ring)]"}`}
            >
              <Stage>
                <SlideView def={s} step={s.steps ?? 0} live={false} index={i} total={slides.length} />
              </Stage>
            </div>
            <div className="mt-2 flex items-baseline gap-2 text-[13px]">
              <span className="font-mono text-[var(--muted)]">{String(i + 1).padStart(2, "0")}</span>
              <span className="text-[var(--ink)]">{s.title}</span>
            </div>
          </button>
        ))}
      </div>
    </motion.div>
  );
}

function NavHint() {
  const [show, setShow] = useState(true);
  useEffect(() => {
    const t = setTimeout(() => setShow(false), 4000);
    return () => clearTimeout(t);
  }, []);
  return (
    <div
      className="pointer-events-none fixed bottom-4 left-1/2 z-[70] -translate-x-1/2 whitespace-nowrap rounded-sm border border-[var(--line-strong)] bg-[var(--panel)] px-3 py-1.5 font-mono text-[12px] text-[var(--muted)] transition-opacity duration-700"
      style={{ opacity: show ? 1 : 0 }}
    >
      ← → step · shift+→ skip · o overview · p presenter · d dark · f fullscreen · b blank
    </div>
  );
}

function PresenterView({ slides, pos, onStep }: { slides: SlideDef[]; pos: Position; onStep: (d: 1 | -1) => void }) {
  const [start] = useState(() => Date.now());
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  const def = slides[pos.slide];
  const steps = def.steps ?? 0;
  const nextPos: Position | null =
    pos.step < steps ? { slide: pos.slide, step: pos.step + 1 } : pos.slide < slides.length - 1 ? { slide: pos.slide + 1, step: 0 } : null;
  const secs = Math.floor((now - start) / 1000);
  const elapsed = `${String(Math.floor(secs / 60)).padStart(2, "0")}:${String(secs % 60).padStart(2, "0")}`;

  return (
    <div className="fixed inset-0 grid grid-cols-[1.6fr_1fr] grid-rows-[auto_1fr] gap-5 bg-[var(--bg)] p-5 text-[var(--ink)]">
      <div className="col-span-2 flex items-center gap-6 border-b border-[var(--line)] pb-3 font-mono text-[13px] text-[var(--muted)]">
        <span>
          slide <span className="text-[var(--ink)]">{pos.slide + 1}</span> / {slides.length}
        </span>
        <span>
          step <span className="text-[var(--ink)]">{pos.step}</span> / {steps}
        </span>
        <span className="text-[var(--ink)]">{def.section}</span>
        <span className="ml-auto text-[22px] tabular-nums text-[var(--ink)]">{elapsed}</span>
        <button className="btn btn-secondary btn-sm" onClick={() => onStep(-1)}>
          ← prev
        </button>
        <button className="btn btn-primary btn-sm" onClick={() => onStep(1)}>
          next →
        </button>
      </div>
      <div className="flex min-h-0 flex-col gap-2">
        <div className="label">Current</div>
        <div className="aspect-video border border-[var(--line-strong)]">
          <Stage>
            <SlideView def={def} step={pos.step} live index={pos.slide} total={slides.length} />
          </Stage>
        </div>
      </div>
      <div className="flex min-h-0 flex-col gap-4">
        <div className="label">Next</div>
        <div className="aspect-video border border-[var(--line-strong)] opacity-90">
          {nextPos ? (
            <Stage>
              <SlideView def={slides[nextPos.slide]} step={nextPos.step} live={false} index={nextPos.slide} total={slides.length} />
            </Stage>
          ) : (
            <div className="grid h-full place-items-center text-[var(--muted)]">End of deck</div>
          )}
        </div>
        <div className="label">Notes</div>
        <div className="thin-scroll min-h-0 flex-1 overflow-y-auto whitespace-pre-line text-[19px] leading-relaxed text-[var(--fg)]">
          {def.notes ?? "—"}
        </div>
      </div>
    </div>
  );
}
