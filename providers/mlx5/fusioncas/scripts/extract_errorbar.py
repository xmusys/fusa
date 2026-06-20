import os
import re
from typing import Dict, List
from glob import glob

# config
THREAD_NUMS = [32, 28, 24, 20, 16, 12, 8]  # [32, 28, 24, 20, 16, 12, 8]
THRESHOLDS = [0, 50000, 80000, 99999, 1000000]
STRIDES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]
DEPTHS = [2]
CLIENT_IPS = ["192.168.6.4", "192.168.6.3", "192.168.6.5", "192.168.6.6"]
YCSB_TYPES = ["update", "u90r10", "u80r20", "u70r30", "u60r40", "u50r50", "u40r60", "u30r70", "u20r80", "u10r90"]  # ["update", "u90r10", "u80r20", "u70r30", "u60r40", "ycsb_a", "u40r60", "u30r70", "u20r80", "u10r90"]
exp_type = ["enable"]
exp_prefix = "errorbar/cas_retry"
distribution_type = ["zipfian"]

times = 60

# extract config and timestamp from log filename
def parse_log_filename(filename: str) -> Dict[str, str]:
    pattern = r"latency_([^_]+)_([^_]+)_([^_]+)_(\d+\.\d+\.\d+\.\d+)_(\d+)_(\d+)_(\d+)_client(\d+)_(.+)\.log"
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
            "timestamp": match.group(9)
        }
    return None

# generate log file paths, group by config and timestamp
def generate_config_groups() -> Dict[str, List[str]]:
    config_groups = {}
    log_dir_base = "../exp_log"
    
    # scan all log files
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

    # filter matching configs
    for exp_t in exp_type:
        for dist_t in distribution_type:
            for thread_num in THREAD_NUMS:
                for depth in DEPTHS:
                    for threshold in THRESHOLDS:
                        for ycsb_type in YCSB_TYPES:
                            # find matching config and timestamp
                            for config_key, files_dict in all_log_files.items():
                                if (exp_t in config_key and dist_t in config_key and ycsb_type in config_key and
                                    str(thread_num) in config_key and str(depth) in config_key and str(threshold) in config_key):
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

# extract latency data from log file
def extract_latency_metrics(log_file: str) -> Dict[str, float]:
    metrics = {"Avg": None, "P50": None, "P99": None, "P999": None, "attempts": None, "IOPS": None}
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read()

            # extract attempts
            attempts_match = re.search(r"total attempts:\s*(\d+)", content)
            if attempts_match:
                metrics["attempts"] = int(attempts_match.group(1))
                metrics["IOPS"] = metrics["attempts"] / (times * 1000000)  # IOPS = attempts / 10

            # extract latency (Avg, P50, P99, P999)
            latency_match = re.search(
                r"client_report\(\): ([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", content
            )
            if latency_match:
                metrics["Avg"] = float(latency_match.group(1))
                metrics["P50"] = float(latency_match.group(2))
                metrics["P99"] = float(latency_match.group(3))
                metrics["P999"] = float(latency_match.group(4))
                
    except FileNotFoundError:
print(f"file {log_file} not found")
    except Exception as e:
print(f" {log_file} error: {e}")
    
    return metrics

# main function
def main():
    config_groups = generate_config_groups()
    results = {}

    # create output directory
    save_dir = f"extract_result/{exp_prefix}"
    os.makedirs(save_dir, exist_ok=True)

    # iterate over each config
    for config_key, log_files in config_groups.items():
        total_latency = {"Avg": 0.0, "P50": 0.0, "P99": 0.0, "P999": 0.0}
        total_IOPS = 0.0  # total IOPS
        valid_files = 0
        client_metrics = []  # store metrics per machine
        
        # process 4 machines per group
        for i, log_file in enumerate(log_files):
            metrics = extract_latency_metrics(log_file)
            client_ip = CLIENT_IPS[i]
            client_metrics.append((client_ip, metrics))

            # accumulate data
            if metrics["Avg"] is not None:
                total_latency["Avg"] += metrics["Avg"]
                total_latency["P50"] += metrics["P50"]
                total_latency["P99"] += metrics["P99"]
                total_latency["P999"] += metrics["P999"]
                valid_files += 1

            # accumulate IOPS
            if metrics["IOPS"] is not None:
                total_IOPS += metrics["IOPS"]

        if valid_files == 0:
            continue
        # compute average
        if valid_files > 0:
            total_latency["Avg"] /= valid_files
            total_latency["P50"] /= valid_files
            total_latency["P99"] /= valid_files
            total_latency["P999"] /= valid_files

        # store result
        results[config_key] = {
            "Avg": total_latency["Avg"] if valid_files > 0 else None,
            "P50": total_latency["P50"] if valid_files > 0 else None,
            "P99": total_latency["P99"] if valid_files > 0 else None,
            "P999": total_latency["P999"] if valid_files > 0 else None,
            "total_IOPS": total_IOPS if valid_files > 0 else None,
            "valid_files": valid_files,
            "client_metrics": client_metrics
        }

        # save to file
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

                # write sum
                f.write(f"\nTotal Avg Latency: {results[config_key]['Avg']:.3f} us (based on  {valid_files}  machines)\n")
                f.write(f"Total P50 Latency: {results[config_key]['P50']:.3f} us\n")
                f.write(f"Total P99 Latency: {results[config_key]['P99']:.3f} us\n")
                f.write(f"Total P999 Latency: {results[config_key]['P999']:.3f} us\n")
                f.write(f"Total IOPS: {results[config_key]['total_IOPS']:.3f} M/s\n")

    # output to console
print("config machines latency data:: ")
    for config_key, metrics in results.items():
        print(f"Config: {config_key}")
        if metrics["valid_files"] == 0:
            print("  no valid log files")
        else:
            print(f"  Total Avg Latency: {metrics['Avg']:.3f} us (based on  {metrics['valid_files']}  machines)")
            print(f"  Total P50 Latency: {metrics['P50']:.3f} us")
            print(f"  Total P99 Latency: {metrics['P99']:.3f} us")
            print(f"  Total P999 Latency: {metrics['P999']:.3f} us")
            print(f"  Total IOPS: {metrics['total_IOPS']:.3f} M/s")
        print()

if __name__ == "__main__":
    main()