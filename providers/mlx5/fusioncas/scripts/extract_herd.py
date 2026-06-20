import os
import re
from typing import Dict, List

# config
THREAD_NUMS = [32]
THRESHOLDS = [0]
STRIDES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]
DEPTHS = [2]
CLIENT_IPS = ["192.168.6.4", "192.168.6.3", "192.168.6.5", "192.168.6.6"]
YCSB_TYPES = ["update", "u90r10", "u80r20", "u70r30", "u60r40", "ycsb_a", "u40r60", "u30r70", "u20r80", "u10r90"]
exp_type = ["enable", "disable"]
exp_prefix = "herd_exp"
distribution_type = ["uniform", "zipfian"]

# fileconfig
def generate_config_groups() -> Dict[str, List[str]]:
    config_groups = {}
    for exp_t in exp_type:
        for dist_t in distribution_type:  # add distribution_type loop
            for thread_num in THREAD_NUMS:
                for depth in DEPTHS:
                    for threshold in THRESHOLDS:
                        for ycsb_type in YCSB_TYPES:
# config_key, include exp_t and dist_t
                            config_key = f"{exp_t}_{dist_t}_{ycsb_type}_{thread_num}_{depth}_{threshold}"
                            log_files = []
                            for i, client_ip in enumerate(CLIENT_IPS):
                                log_file = f"../data/{client_ip}/{exp_prefix}/{exp_t}_{dist_t}_{ycsb_type}_{client_ip}_{thread_num}_{depth}_{threshold}_client{i+1}.log"
                                log_files.append(log_file)
                            config_groups[config_key] = log_files
    return config_groups

# fileextract IOPS total retry cnt
def extract_metrics(log_file: str) -> Dict[str, float]:
    metrics = {"IOPS": None, "total_retry_cnt": None}
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # extract IOPS
            iops_match = re.search(r"IOPS=([\d.]+)\s*M/s", content)
            if iops_match:
                metrics["IOPS"] = float(iops_match.group(1))
            
            # extract total retry cnt
            retry_match = re.search(r"total retry cnt:\s*(\d+)", content)
            if retry_match:
                metrics["total_retry_cnt"] = int(retry_match.group(1))
                
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
        total_iops = 0.0
        total_retry_cnt = 0
        valid_files = 0
        client_metrics = []  # store metrics per machine
        
        # process 4 machines per group
        for i, log_file in enumerate(log_files):
            metrics = extract_metrics(log_file)
            client_ip = CLIENT_IPS[i]
            client_metrics.append((client_ip, metrics))
            if metrics["IOPS"] is not None:
                total_iops += metrics["IOPS"]
                valid_files += 1
            if metrics["total_retry_cnt"] is not None:
                total_retry_cnt += metrics["total_retry_cnt"]
        
        # store result
        results[config_key] = {
            "total_iops": total_iops if valid_files > 0 else None,
            "total_retry_cnt": total_retry_cnt if total_retry_cnt > 0 else None,
            "valid_files": valid_files,
            "client_metrics": client_metrics
        }

        # save to file
        output_file = os.path.join(save_dir, f"{config_key}.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            if valid_files == 0:
                f.write("no valid log files\n")
            else:
# machines
                for client_ip, metrics in client_metrics:
                    iops = f"{metrics['IOPS']:.3f}" if metrics['IOPS'] is not None else "not found"
                    retry_cnt = metrics["total_retry_cnt"] if metrics["total_retry_cnt"] is not None else "not found"
                    f.write(f"Client {client_ip}: IOPS={iops} M/s, Total Retry Cnt={retry_cnt}\n")
                
                # write sum
                f.write(f"\nTotal IOPS: {results[config_key]['total_iops']:.3f} M/s (based on  {valid_files}  machines)\n")
                f.write(f"Total Retry Cnt: {results[config_key]['total_retry_cnt'] if results[config_key]['total_retry_cnt'] is not None else 'not found'}\n")
    
    # output to console
print("config machines IOPS total retry cnt : ")
    for config_key, metrics in results.items():
        print(f"Config: {config_key}")
        if metrics["valid_files"] == 0:
            print("  no valid log files")
        else:
            print(f"  Total IOPS: {metrics['total_iops']:.3f} M/s (based on  {metrics['valid_files']}  machines)")
            print(f"  Total Retry Cnt: {metrics['total_retry_cnt'] if metrics['total_retry_cnt'] is not None else 'not found'}")
        print()

if __name__ == "__main__":
    main()