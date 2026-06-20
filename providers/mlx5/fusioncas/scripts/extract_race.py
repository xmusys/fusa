import os
import re
from typing import Dict, List
from glob import glob

# config
THREAD_NUMS = [32, 31, 30, 28, 24, 20, 16, 12, 8, 4, 26, 22, 18, 14, 10, 6]
THRESHOLDS = [50000, 80000, 10000000, 999999]
STRIDES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]
DEPTHS = [4, 2]
CLIENT_IPS = ["192.168.6.4", "192.168.6.3", "192.168.6.5", "192.168.6.6"]
YCSB_TYPES = ["ycsb_a25m", "ycsb_a50m"]
exp_type = ["enable"]
exp_prefix = "race/smart"
distribution_type = ["zipfian"]
times = 60

# file
def parse_log_filename(filename: str) -> Dict[str, str]:
# ,
    pattern = r"latency_([^_]+)_([^_]+)_([\w_]+)_(\d+\.\d+\.\d+\.\d+)_(\d+)_(\d+)_(\d+)_client(\d+)_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\.log"
    match = re.match(pattern, os.path.basename(filename))
    if match:
        return {
            "exp_t": match.group(1),
            "dist_t": match.group(2),
            "ycsb_type": match.group(3),
            "client_ip": match.group(4),
            "thread_num": match.group(5),
            "depth": match.group(6),
            "threshold": match.group(7),
            "client_idx": match.group(8),
            "timestamp": match.group(9).replace(" ", "_").replace(":", "-")  # normalize timestamp
        }
print(f"file: {filename}")
    return None

# generate log file paths, group by config and timestamp
def generate_config_groups() -> Dict[str, List[str]]:
    config_groups = {}
    log_dir_base = "../exp_log"
    
    all_log_files = {}
    for client_ip in CLIENT_IPS:
        log_dir = os.path.join(log_dir_base, client_ip, exp_prefix)
        log_files = glob(os.path.join(log_dir, "latency_*.log"))
        for log_file in log_files:
            parsed = parse_log_filename(log_file)
            if parsed:
                config_key = f"{parsed['exp_t']}_{parsed['dist_t']}_{parsed['ycsb_type']}_{parsed['thread_num']}_{parsed['depth']}_{parsed['threshold']}_{parsed['timestamp']}"
                if config_key not in all_log_files:
                    all_log_files[config_key] = {}
                all_log_files[config_key][parsed['client_ip']] = log_file

    for exp_t in exp_type:
        for dist_t in distribution_type:
            for thread_num in THREAD_NUMS:
                for depth in DEPTHS:
                    for threshold in THRESHOLDS:
                        for ycsb_type in YCSB_TYPES:
                            for config_key, files_dict in all_log_files.items():
                                parsed = parse_log_filename(list(files_dict.values())[0])
                                if parsed and (exp_t == parsed['exp_t'] and dist_t == parsed['dist_t'] and ycsb_type == parsed['ycsb_type'] and
                                    str(thread_num) == parsed['thread_num'] and str(depth) == parsed['depth'] and str(threshold) == parsed['threshold']):
                                    log_files = []
                                    valid = True
                                    for client_ip in CLIENT_IPS:
                                        if client_ip in files_dict:
                                            log_files.append(files_dict[client_ip])
                                        else:
                                            valid = False
                                            break
                                    if valid and len(log_files) == 4:
                                        config_groups[config_key] = log_files
    
    return config_groups

# extract latency data function
def extract_latency_metrics(log_file: str) -> Dict[str, float]:
    metrics = {"Avg": None, "P50": None, "P99": None, "P999": None, "attempts": None, "IOPS": None}
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read()

            dump_match = re.search(
                r"dump_result\(\): HashTable: workload = ([^,]+), #thread = (\d+), #coro_per_thread = (\d+), "
                r"key length = (\d+), value length = (\d+), max key = (\d+), "
                r"throughput = ([\d.]+) M, Avg latency = ([\d.-]+) us, P50 latency = ([\d.-]+) us, "
                r"P99 latency = ([\d.-]+) us, P999 latency = ([\d.-]+) us",
                content
            )
            if dump_match:
                metrics["workload"] = dump_match.group(1)
                metrics["thread_num"] = int(dump_match.group(2))
                metrics["coro_per_thread"] = int(dump_match.group(3))
                metrics["key_length"] = int(dump_match.group(4))
                metrics["value_length"] = int(dump_match.group(5))
                metrics["max_key"] = int(dump_match.group(6))
                metrics["IOPS"] = float(dump_match.group(7))
                metrics["Avg"] = float(dump_match.group(8))
                metrics["P50"] = float(dump_match.group(9))
                metrics["P99"] = float(dump_match.group(10)) if float(dump_match.group(10)) >= 0 else None
                metrics["P999"] = float(dump_match.group(11)) if float(dump_match.group(11)) >= 0 else None

                if metrics["IOPS"] is not None:
                    metrics["attempts"] = int(metrics["IOPS"] * times * 1000000)

    except FileNotFoundError:
print(f"file {log_file} not found")
    except Exception as e:
print(f" {log_file} error: {e}")
    
    return metrics

# main function
def main():
    config_groups = generate_config_groups()
    results = {}

    save_dir = f"extract_result/{exp_prefix}"
    os.makedirs(save_dir, exist_ok=True)

    for config_key, log_files in config_groups.items():
        total_latency = {"Avg": 0.0, "P50": 0.0, "P99": 0.0, "P999": 0.0}
        total_IOPS = 0.0
        valid_files = 0
        valid_p99_files = 0
        valid_p999_files = 0
        client_metrics = []
        
        for i, log_file in enumerate(log_files):
            metrics = extract_latency_metrics(log_file)
            client_ip = CLIENT_IPS[i]
            client_metrics.append((client_ip, metrics))

            if metrics["P50"] is not None:
                total_latency["Avg"] += metrics["Avg"] if metrics["Avg"] is not None else 0.0
                total_latency["P50"] += metrics["P50"]
                valid_files += 1
                if metrics["P99"] is not None:
                    total_latency["P99"] += metrics["P99"]
                    valid_p99_files += 1
                if metrics["P999"] is not None:
                    total_latency["P999"] += metrics["P999"]
                    valid_p999_files += 1

            if metrics["IOPS"] is not None:
                total_IOPS += metrics["IOPS"]

        if valid_files == 0:
            continue

        if valid_files > 0:
            total_latency["Avg"] = total_latency["Avg"] / valid_files if total_latency["Avg"] > 0 else None
            total_latency["P50"] = total_latency["P50"] / valid_files
            total_latency["P99"] = total_latency["P99"] / valid_p99_files if valid_p99_files > 0 else None
            total_latency["P999"] = total_latency["P999"] / valid_p999_files if valid_p999_files > 0 else None

        results[config_key] = {
            "Avg": total_latency["Avg"],
            "P50": total_latency["P50"],
            "P99": total_latency["P99"],
            "P999": total_latency["P999"],
            "total_IOPS": total_IOPS if valid_files > 0 else None,
            "valid_files": valid_files,
            "client_metrics": client_metrics
        }

        output_file = os.path.join(save_dir, f"latency_{config_key}.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            if valid_files == 0:
                f.write("no valid log files\n")
            else:
                for client_ip, metrics in client_metrics:
                    avg = f"{metrics['Avg']:.3f}" if metrics['Avg'] is not None else "not found"
                    p50 = f"{metrics['P50']:.3f}" if metrics['P50'] is not None else "not found"
                    p99 = f"{metrics['P99']:.3f}" if metrics['P99'] is not None else "not found"
                    p999 = f"{metrics['P999']:.3f}" if metrics['P999'] is not None else "not found"
                    iops = f"{metrics['IOPS']:.3f}" if metrics['IOPS'] is not None else "not found"
                    f.write(f"Client {client_ip}: Avg={avg} us, P50={p50} us, P99={p99} us, P999={p999} us, IOPS={iops} M/s\n")

                f.write(f"\nTotal Avg Latency: {results[config_key]['Avg']:.3f} us\n" if results[config_key]['Avg'] is not None else "Total Avg Latency: not found\n")
                f.write(f"Total P50 Latency: {results[config_key]['P50']:.3f} us (based on  {valid_files}  machines)\n")
                f.write(f"Total P99 Latency: {results[config_key]['P99']:.3f} us\n" if results[config_key]['P99'] is not None else "Total P99 Latency: not found\n")
                f.write(f"Total P999 Latency: {results[config_key]['P999']:.3f} us\n" if results[config_key]['P999'] is not None else "Total P999 Latency: not found\n")
                f.write(f"Total IOPS: {results[config_key]['total_IOPS']:.3f} M/s\n")

print("config machines latency data:: ")
    for config_key, metrics in results.items():
        print(f"Config: {config_key}")
        if metrics["valid_files"] == 0:
            print("  no valid log files")
        else:
            print(f"  Total Avg Latency: {metrics['Avg']:.3f} us" if metrics['Avg'] is not None else "  Total Avg Latency: not found")
            print(f"  Total P50 Latency: {metrics['P50']:.3f} us (based on  {metrics['valid_files']}  machines)")
            print(f"  Total P99 Latency: {metrics['P99']:.3f} us" if metrics['P99'] is not None else "  Total P99 Latency: not found")
            print(f"  Total P999 Latency: {metrics['P999']:.3f} us" if metrics['P999'] is not None else "  Total P999 Latency: not found")
            print(f"  Total IOPS: {metrics['total_IOPS']:.3f} M/s")
        print()

if __name__ == "__main__":
    main()