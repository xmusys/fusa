#!/usr/bin/env python3
import argparse
import os
import sys
from collections import Counter, defaultdict

# Txn type name mappings from traces/README
TATP_TYPES = {
    1: "GET_SUBSCRIBER_DATA",
    2: "GET_NEW_DESTINATION",
    3: "GET_ACCESS_DATA",
    4: "UPDATE_SUBSCRIBER_DATA",
    5: "UPDATE_LOCATION",
    6: "INSERT_CALL_FORWARDING",
    7: "DELETE_CALL_FORWARDING",
}

TPCC_TYPES = {
    1: "NEW_ORDER",
    2: "PAYMENT",
    3: "ORDER_STATUS",
    4: "DELIVERY",
    5: "STOCK_LEVEL",
}

def guess_benchmark_type(filename: str):
    f = filename.lower()
    if "tatp" in f:
        return "tatp"
    if "tpcc" in f:
        return "tpcc"
    return "unknown"


def bucketize(n: int) -> str:
    if n <= 1:
        return "1"
    if n <= 3:
        return "2-3"
    if n <= 7:
        return "4-7"
    if n <= 15:
        return "8-15"
    if n <= 31:
        return "16-31"
    return "32+"


def analyze_csv(path: str):
    bench = guess_benchmark_type(os.path.basename(path))
    type_names = TATP_TYPES if bench == "tatp" else TPCC_TYPES if bench == "tpcc" else {}

    line_count = 0
    txn_count = 0
    txn_type_cnt = Counter()
    shared_cnt = 0  # LockType=1
    excl_cnt = 0    # LockType=2
    key_min = None
    key_max = None
    key_counter = Counter()

    # locks per transaction stats
    cur_txn_id = None
    cur_locks = 0
    locks_total = 0
    locks_max = 0
    buckets = Counter()

    # CSV format: TxnID,0,TxnType,LockID,LockType\n
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                a, _, t, k, lt = line.split(",")
                txn_id = int(a)
                txn_type = int(t)
                key = int(k)
                ltype = int(lt)
            except Exception:
                # malformed line; skip
                continue

            line_count += 1

            # per-line aggregations
            txn_type_cnt[txn_type] += 1
            if ltype == 1:
                shared_cnt += 1
            elif ltype == 2:
                excl_cnt += 1

            if key_min is None or key < key_min:
                key_min = key
            if key_max is None or key > key_max:
                key_max = key
            key_counter[key] += 1

            # per-transaction accounting
            if cur_txn_id is None:
                cur_txn_id = txn_id
                cur_locks = 0
            if txn_id != cur_txn_id:
                # flush previous txn
                txn_count += 1
                locks_total += cur_locks
                locks_max = max(locks_max, cur_locks)
                buckets[bucketize(cur_locks)] += 1
                # start new txn
                cur_txn_id = txn_id
                cur_locks = 0
            cur_locks += 1

    # flush last txn if any lines were read
    if line_count > 0 and cur_txn_id is not None:
        txn_count += 1
        locks_total += cur_locks
        locks_max = max(locks_max, cur_locks)
        buckets[bucketize(cur_locks)] += 1

    locks_avg = (locks_total / txn_count) if txn_count > 0 else 0.0

    # build type distribution pretty names
    type_dist = []
    for ty, cnt in sorted(txn_type_cnt.items()):
        name = type_names.get(ty, str(ty))
        type_dist.append((name, cnt))

    top_keys = key_counter.most_common(10)

    # uniformity metrics over keys
    unique_keys = len(key_counter)
    mean_per_key = (line_count / unique_keys) if unique_keys > 0 else 0.0
    max_per_key = max(key_counter.values()) if key_counter else 0
    # coefficient of variation (std/mean)
    if unique_keys > 0 and mean_per_key > 0:
        import math
        s2 = 0.0
        for c in key_counter.values():
            d = c - mean_per_key
            s2 += d * d
        var = s2 / unique_keys
        std = math.sqrt(var)
        cv = std / mean_per_key
    else:
        cv = 0.0
    max_mean_ratio = (max_per_key / mean_per_key) if mean_per_key > 0 else 0.0
    # Heuristic: close to uniform if dispersion is small
    is_uniform = (cv <= 0.5 and max_mean_ratio <= 2.0)

    return {
        "file": os.path.basename(path),
        "benchmark": bench,
        "line_count": line_count,
        "txn_count": txn_count,
        "type_distribution": type_dist,
        "locks_avg": locks_avg,
        "locks_max": locks_max,
        "locks_buckets": dict(buckets),
        "shared_cnt": shared_cnt,
        "exclusive_cnt": excl_cnt,
        "key_min": key_min,
        "key_max": key_max,
        "top_keys": top_keys,
        "unique_keys": unique_keys,
        "mean_per_key": mean_per_key,
        "max_per_key": max_per_key,
        "cv": cv,
        "max_mean_ratio": max_mean_ratio,
        "is_uniform": is_uniform,
    }


def main():
    parser = argparse.ArgumentParser(description="Analyze SHIFTLOCK CSV traces")
    parser.add_argument("--dir", default="/home/dell/sqs/shiftlock/traces", help="Directory containing CSV traces")
    parser.add_argument("--top", type=int, default=10, help="Top-N hot keys to print")
    args = parser.parse_args()

    if not os.path.isdir(args.dir):
        print(f"Directory not found: {args.dir}", file=sys.stderr)
        return 1

    files = [os.path.join(args.dir, f) for f in os.listdir(args.dir) if f.endswith('.csv')]
    files.sort()
    if not files:
        print("No CSV files found.")
        return 0

    # Print a legend once
    print("Legend:")
    print("  Lines: total CSV rows (each is one lock record)")
    print("  Txns: number of transactions after merging rows by TxnID")
    print("  Locks per txn: average and max number of locks per transaction")
    print("  Locks buckets: histogram of locks-per-txn")
    print("  Shared(exclusive): counts of LockType=1 (shared) and LockType=2 (exclusive)")
    print("  Key range: min and max LockID")
    print("  Uniform: heuristic based on key-access dispersion (CV and max/mean)")
    print()

    for fp in files:
        r = analyze_csv(fp)
        print("-" * 80)
        print(f"File: {r['file']}  (benchmark: {r['benchmark']})")
        print(f"Lines: {r['line_count']:,}  Txns: {r['txn_count']:,}")
        print(f"TxnType distribution:")
        for name, cnt in r['type_distribution']:
            print(f"  {name:18s} : {cnt:,}")
        print(f"Locks per txn: avg={r['locks_avg']:.2f}  max={r['locks_max']}")
        print(f"Locks buckets:")
        for b, c in sorted(r['locks_buckets'].items(), key=lambda x: x[0]):
            print(f"  {b:>5s} : {c:,}")
        print(f"Shared(exclusive) locks: {r['shared_cnt']:,} ({r['exclusive_cnt']:,})")
        print(f"Key range: [{r['key_min']}, {r['key_max']}]")
        # uniformity summary
        print(f"Key stats: unique={r['unique_keys']:,}  mean/Key={r['mean_per_key']:.2f}  max/Key={r['max_per_key']}")
        print(f"Dispersion: CV={r['cv']:.2f}  max/mean={r['max_mean_ratio']:.2f}  Uniform={r['is_uniform']}")
        print(f"Top-{args.top} keys:")
        for key, cnt in r['top_keys'][:args.top]:
            print(f"  key={key:10d}  cnt={cnt:,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
