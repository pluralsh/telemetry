"""Run a fuzz session without pytest, e.g. against remote S3-backed stacks:

PYTHONPATH=tests/regression FUZZ_STACK=external python -m harness.fuzz metrics
"""

from __future__ import annotations

import sys

from .logs import LogsFuzz
from .metrics import MetricsFuzz
from .runner import run_fuzz
from .traces import TracesFuzz

PRODUCTS = {product.name: product for product in (LogsFuzz, MetricsFuzz, TracesFuzz)}


def main(argv: list[str]) -> int:
    if len(argv) != 1 or argv[0] not in PRODUCTS:
        print(
            f"usage: python -m harness.fuzz {{{','.join(PRODUCTS)}}}", file=sys.stderr
        )
        return 2
    summary = run_fuzz(PRODUCTS[argv[0]])
    for reason in summary.reasons:
        print(f"failure: {reason}", file=sys.stderr)
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
