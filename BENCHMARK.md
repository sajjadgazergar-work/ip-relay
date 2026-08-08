# Benchmark — does ip-relay actually work?

**Yes. Here are the real numbers, measured on the live system.**

Measured against `opencode.ai/zen/v1` (model `deepseek-v4-flash-free`), from a server whose own IP was already burned for the free tier — i.e. **every successful request went through a rotating proxy IP**.

Run on 2026-08-08 against a proxy pool of 9–40 free HTTP proxies, with the rotation engine active (`RELAY_PROXY_TIMEOUT=25s`, low-pool auto-refresh on).

## Results

### Light load — 10 parallel × 5 rounds (50 requests)

| Round | Success | Avg latency | Max latency |
|---|---|---|---|
| 1 | 10/10 (100%) | 1.42s | 1.62s |
| 2 | 10/10 (100%) | 1.61s | 1.69s |
| 3 | 10/10 (100%) | 1.49s | 1.56s |
| 4 | 10/10 (100%) | 1.70s | 1.78s |
| 5 | 10/10 (100%) | 1.90s | 2.11s |

**Total: 50/50 (100%) · avg 1.62s**

### Medium load — 20 parallel × 10 rounds (200 requests)

**Total: 200/200 (100%) · avg 1.77s** · zero errors

### Heavy load — 30 parallel × 10 rounds (300 requests)

| Round | Success | Avg latency | Max latency |
|---|---|---|---|
| 1 | 30/30 (100%) | 14.57s | 54.86s |
| 2 | 30/30 (100%) | 11.91s | 35.11s |
| 3 | 30/30 (100%) | 7.31s | 22.15s |
| 4 | 30/30 (100%) | 6.31s | 17.30s |
| 5 | 30/30 (100%) | 6.68s | 22.86s |
| 6 | 30/30 (100%) | 7.33s | 19.47s |
| 7 | 30/30 (100%) | 4.02s | 11.43s |
| 8 | 30/30 (100%) | 7.32s | 19.72s |
| 9 | 30/30 (100%) | 5.02s | 13.81s |
| 10 | 30/30 (100%) | 5.79s | 20.51s |

**Total: 300/300 (100%) · avg 7.63s** · latency improves as the pool finds fresh IPs

### Brutal load — 50 parallel × 8 rounds (400 requests)

**Total: 399/400 (99.8%) · avg 10.61s** · 1 timeout in the heaviest round

## What the rotation counters prove

During the brutal run, the health endpoint showed:

```
requests: 748, rotations: 99, lane_failures: 98
```

- **99 rotations** = the pool hit 99 per-IP quota errors and **automatically switched to fresh IPs**, all while serving requests
- **98 lane failures** = dead/slow proxies were detected and parked
- **Pool self-healed** from burn-down back up as background refresh found new proxies (low-pool auto-refresh)

Without the rotator, all 750+ requests from one IP would have died at the first quota limit (~dozens of requests).

## How to reproduce

```bash
# point at your running relay
python3 loadtest.py 10 5     # light
python3 loadtest.py 30 10    # heavy
python3 loadtest.py 50 8     # brutal
```

`loadtest.py` is in the repo root. It reports per-round success, latency, and error breakdown, then the pool state after.

## Caveats

- Free proxy pools are **volatile** — numbers vary by hour and by pool provider
- Latency is dominated by proxy speed, not the relay (relay adds ~0.2s)
- Under extreme sustained load the pool *can* run dry (all IPs burned, refresh lagging) — that's when you get timeouts, not wrong answers. The low-pool auto-refresh mitigates this.
- Don't hammer the free tier. It's free because it's shared.
