import { motion } from "motion/react";
import { useStep } from "../deck/context";
import { EASE } from "../ui/primitives";

/** Upper-bound BM25 score per posting block (128 postings each), in on-disk order. */
const BOUNDS = [3.1, 7.8, 2.2, 5.9, 9.4, 1.6, 4.4, 8.6, 2.9, 6.7, 1.1, 3.8, 5.2, 2.5];
const BEST = [8.9, 8.1, 7.4, 6.9, 6.2];
const K_THRESHOLD = 6.2;

const sorted = BOUNDS.map((b, i) => ({ b, i })).sort((x, y) => y.b - x.b);
const MAXH = 360;

export function Bm25Blocks() {
  const { step } = useStep();
  const order = step >= 1 ? sorted.map((s) => s.i) : BOUNDS.map((_, i) => i);
  const scored = step >= 2;
  return (
    <div className="flex h-full flex-col">
      <div className="flex items-baseline gap-6">
        <span className="code rounded-[3px] bg-ink px-4 py-1.5 text-[24px] text-bg">{'{app="checkout"} | match "payment timeout"'}</span>
        <span className="eyebrow text-[19px]">top 5 by BM25</span>
      </div>

      <div className="relative mt-10 flex-1">
        <div className="absolute inset-x-0 bottom-[44px] border-t border-line-strong" />
        <motion.div
          className="absolute inset-x-0 border-t-2 border-dashed border-accent"
          initial={false}
          animate={{ bottom: 44 + (scored ? (K_THRESHOLD / 10) * MAXH : 0), opacity: scored ? 1 : 0 }}
          transition={{ duration: 1.2, ease: EASE, delay: 0.6 }}
        >
          <span className="code absolute -top-9 right-0 text-[18px] text-accent-text">5th best score so far</span>
        </motion.div>

        <div className="absolute inset-x-0 bottom-0 flex h-full items-end justify-between">
          {order.map((idx, pos) => {
            const b = BOUNDS[idx];
            const skip = scored && b < K_THRESHOLD;
            return (
              <motion.div key={idx} layout transition={{ layout: { duration: 0.8, ease: EASE } }} className="flex w-[84px] flex-col items-center">
                <motion.div
                  className="relative w-full rounded-t-[3px]"
                  style={{ height: (b / 10) * MAXH }}
                  initial={false}
                  animate={{
                    background: skip ? "var(--bg-subtle)" : "color-mix(in srgb, var(--accent-text) 22%, var(--panel))",
                    borderColor: skip ? "var(--line)" : "var(--accent-text)",
                    opacity: skip ? 0.55 : 1,
                  }}
                  transition={{ delay: scored ? 0.8 + pos * 0.12 : 0 }}
                >
                  <div className="absolute inset-0 rounded-t-[3px] border-[1.5px] border-[inherit]" />
                  <span className="code absolute -top-8 left-1/2 -translate-x-1/2 text-[17px] text-muted">{b.toFixed(1)}</span>
                  {skip && (
                    <motion.span initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.9 + pos * 0.12 }} className="code absolute inset-x-0 bottom-3 text-center text-[14px] text-faint">
                      skip
                    </motion.span>
                  )}
                </motion.div>
                <span className="code mt-3 h-[30px] text-[15px] text-faint">#{idx}</span>
              </motion.div>
            );
          })}
        </div>
      </div>

      <p className="mt-6 max-w-[1500px] text-[24px] leading-snug text-fg">
        {step === 0 && "Each posting block (128 postings) records an upper bound on any score inside it. That bound is cheap to read from the directory page."}
        {step === 1 && "Blocks are visited best-bound first."}
        {step === 2 && `Once five results beat ${K_THRESHOLD}, no remaining block can make the cut. ${BOUNDS.filter((b) => b < K_THRESHOLD).length} of ${BOUNDS.length} blocks are never fetched from object storage. Best scores found: ${BEST.join(", ")}.`}
      </p>
    </div>
  );
}
