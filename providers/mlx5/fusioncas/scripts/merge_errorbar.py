import os
import re
from collections import defaultdict
from statistics import mean
from glob import glob

# dir
source_dir = "extract_result/errorbar/cas_retry"
# dir
RESULT_DIR = "extract_result/errorbar/"

# fileconfig
def parse_filename(filename: str) -> dict:
# file
    pattern = r"latency_([^_]+)_([^_]+)_([^_]+)_(\d+)_(\d+)_(\d+)_\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\.txt"
    match = re.match(pattern, os.path.basename(filename))
    if match:
        return {
            "exp_t": match.group(1),      # enable
            "dist_t": match.group(2),     # zipfian
            "ycsb_type": match.group(3),  # update
            "thread_num": match.group(4), # 32
            "depth": match.group(5),      # 2
            "threshold": match.group(6)   # 10000000
        }
    return None

# file
def extract_metrics(file_path: str) -> dict:
    metrics = {
        "Avg": None,
        "P50": None,
        "P99": None,
        "P999": None,
        "IOPS": None,
"valid_experiments": 1 # files
    }
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

#
            avg_match = re.search(r"Total Avg Latency: ([\d.]+) us", content)
            p50_match = re.search(r"Total P50 Latency: ([\d.]+) us", content)
            p99_match = re.search(r"Total P99 Latency: ([\d.]+) us", content)
            p999_match = re.search(r"Total P999 Latency: ([\d.]+) us", content)
            iops_match = re.search(r"Total IOPS: ([\d.]+) M/s", content)

            if avg_match:
                metrics["Avg"] = float(avg_match.group(1))
            if p50_match:
                metrics["P50"] = float(p50_match.group(1))
            if p99_match:
                metrics["P99"] = float(p99_match.group(1))
            if p999_match:
                metrics["P999"] = float(p999_match.group(1))
            if iops_match:
                metrics["IOPS"] = float(iops_match.group(1))

    except FileNotFoundError:
        print(f"file {file_path} not found")
    except Exception as e:
print(f"file {file_path} error: {e}")
    
    return metrics

# main function
def main():
# config
    config_metrics = defaultdict(list)

# file, source_dir
    files = glob(os.path.join(source_dir, "latency_*.txt"))
    for file_path in files:
        parsed = parse_filename(file_path)
        if parsed:
            config_key = f"{parsed['exp_t']}_{parsed['dist_t']}_{parsed['ycsb_type']}_{parsed['thread_num']}_{parsed['depth']}_{parsed['threshold']}"
            metrics = extract_metrics(file_path)
            if metrics["Avg"] is not None and metrics["IOPS"] is not None:
                config_metrics[config_key].append(metrics)

# dir
    output_dir = os.path.join(RESULT_DIR, "merged")
    os.makedirs(output_dir, exist_ok=True)

# config
    for config_key, metrics_list in config_metrics.items():
        if not metrics_list:
print(f"config {config_key} , skip")
            continue

#
        total_latency = {"Avg": [], "P50": [], "P99": [], "P999": []}
        total_IOPS = []
        total_experiments = 0

        for metrics in metrics_list:
            total_latency["Avg"].append(metrics["Avg"])
            total_latency["P50"].append(metrics["P50"])
            total_latency["P99"].append(metrics["P99"])
            total_latency["P999"].append(metrics["P999"])
            total_IOPS.append(metrics["IOPS"])
            total_experiments += metrics["valid_experiments"]

#
        avg_metrics = {
            "Avg": mean(total_latency["Avg"]),
            "P50": mean(total_latency["P50"]),
            "P99": mean(total_latency["P99"]),
            "P999": mean(total_latency["P999"]),
            "IOPS": mean(total_IOPS)
        }

#
        output_file = os.path.join(output_dir, f"latency_{config_key}_merged.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
f.write(f"Merged Metrics ( {len(metrics_list)} files, {total_experiments} ):\n")
            f.write(f"Total Avg Latency: {avg_metrics['Avg']:.3f} us\n")
            f.write(f"Total P50 Latency: {avg_metrics['P50']:.3f} us\n")
            f.write(f"Total P99 Latency: {avg_metrics['P99']:.3f} us\n")
            f.write(f"Total P999 Latency: {avg_metrics['P999']:.3f} us\n")
            f.write(f"Total IOPS: {avg_metrics['IOPS']:.3f} M/s\n")

#
        print(f"config : {config_key}")
print(f" Total Avg Latency: {avg_metrics['Avg']:.3f} us ( {len(metrics_list)} files, {total_experiments} )")
        print(f"  Total P50 Latency: {avg_metrics['P50']:.3f} us")
        print(f"  Total P99 Latency: {avg_metrics['P99']:.3f} us")
        print(f"  Total P999 Latency: {avg_metrics['P999']:.3f} us")
        print(f"  Total IOPS: {avg_metrics['IOPS']:.3f} M/s")
        print()

if __name__ == "__main__":
    main()