import { motion } from "motion/react";
import { useStep } from "../deck/context";
import { EASE } from "../ui/primitives";

const VALUES = [23.41, 23.47, 23.52, 23.39, 23.61, 23.45, 23.5, 23.58];
const INTS = VALUES.map((v) => Math.round(v * 100));
const MIN = Math.min(...INTS);
const OFFS = INTS.map((v) => v - MIN);
const WIDTH = Math.ceil(Math.log2(Math.max(...OFFS) + 1));

const ROW = ["IEEE-754 doubles", "× 10^2 → exact integers", "− min (frame of reference)", `bit-packed · ${WIDTH} bits / lane`];

function Bits({ n, color, on, delay = 0 }: { n: number; color: string; on: boolean; delay?: number }) {
  return (
    <div className="flex gap-[2px]">
      {Array.from({ length: n }, (_, i) => (
        <motion.span
          key={i}
          className="block h-[22px] w-[3px]"
          style={{ background: color }}
          initial={false}
          animate={{ opacity: on ? 0.85 : 0, scaleY: on ? 1 : 0 }}
          transition={{ delay: on ? delay + i * 0.004 : 0, duration: 0.3 }}
        />
      ))}
    </div>
  );
}

export function AlpEncoding() {
  const { step } = useStep();
  const display = (i: number) => (step === 0 ? VALUES[i].toFixed(2) : step === 1 ? String(INTS[i]) : String(OFFS[i]));
  const bits = step >= 3 ? WIDTH : 64;
  const total = bits * VALUES.length;
  return (
    <div className="flex h-full flex-col gap-8">
      <div className="flex items-center gap-3">
        {ROW.map((r, i) => (
          <div key={r} className="flex items-center gap-3">
            <span className={`code rounded-[3px] border px-3 py-1 text-[19px] transition-colors duration-300 ${i === step ? "border-accent bg-accent text-white" : i < step ? "border-line-strong text-ink" : "border-line text-faint"}`}>
              {r}
            </span>
            {i < ROW.length - 1 && <span className="text-faint">→</span>}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-8 gap-4">
        {VALUES.map((_, i) => (
          <div key={i} className="card flex flex-col items-center gap-4 px-3 py-5">
            <motion.div key={`${step}-${i}`} initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: i * 0.05, duration: 0.4, ease: EASE }} className="code text-[34px] text-ink tabular-nums">
              {display(i)}
            </motion.div>
            <div className="flex h-[22px] items-center">
              <Bits n={bits === 64 ? 32 : bits} color={step >= 3 ? "var(--accent-text)" : "var(--faint)"} on delay={i * 0.04} />
              {bits === 64 && <span className="code ml-1 text-[13px] text-faint">×2</span>}
            </div>
            <div className="code text-[15px] text-muted">{bits} bits</div>
          </div>
        ))}
      </div>

      <div className="flex items-end gap-16">
        <div>
          <div className="label text-[15px]">chunk payload</div>
          <motion.div key={total} initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="font-serif text-[96px] leading-none text-ink tabular-nums">
            {total} <span className="text-[40px] text-muted">bits</span>
          </motion.div>
        </div>
        {step >= 3 && (
          <motion.div initial={{ opacity: 0, x: -10 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: 0.3 }} className="pb-3 font-serif text-[64px] italic leading-none text-accent-text">
            {Math.round(512 / total)}× smaller, lossless
          </motion.div>
        )}
      </div>

      <p className="max-w-[1400px] text-[24px] leading-snug text-fg">
        {step === 0 && "Most metric values are decimals that a human or exporter printed with a few digits, but each is stored as a 64-bit float."}
        {step === 1 && "ALP finds an exponent where every value round-trips bit-for-bit as an integer. Values that don't (NaN stale markers, −0.0, ±Inf) are stored as patched exceptions."}
        {step === 2 && "Subtracting the chunk minimum leaves small offsets, so each lane needs only as many bits as the largest offset."}
        {step === 3 && "Lanes are packed into 64-lane blocks that unpack independently, with SIMD. The encoder falls back to ALP-RD, Gorilla XOR or a constant when they're smaller."}
      </p>
    </div>
  );
}
