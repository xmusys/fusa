import os
import re
from collections import defaultdict
from glob import glob

# define client IPs and directory structure
CLIENT_IPS = ["192.168.6.3", "192.168.6.4", "192.168.6.5", "192.168.6.6"]
EXP_PREFIX = "pcie_atomic/multicore"
exp_type = "enable"
LOG_DIR_BASE = "../exp_log"
TIME = 60  # given time:  60 s

def parse_file_content(file_path):
"""file, extract total_attempts Avg latency"""
    attempts = None
    avg_latency = None
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            # extract total_attempts
            attempts_match = re.search(r"total attempts: (\d+)", content)
            if attempts_match:
                attempts = int(attempts_match.group(1))
            
            # extract Avg latency
            latency_match = re.search(r"client_report\(\): ([\d.]+)\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+", content)
            if latency_match:
                avg_latency = float(latency_match.group(1))
    except FileNotFoundError:
        print(f"file {file_path} not found")
    except Exception as e:
print(f"file {file_path} error: {e}")
    return attempts, avg_latency

def parse_filename(filename):
    """parse filename, extract config and timestamp"""
    pattern = r"latency_enable_zipfian_([^_]+)_(\d+\.\d+\.\d+\.\d+)_(\d+)_(\d+)_(\d+)_(\d+)_client\d+_(.+)\.log"
    match = re.match(pattern, os.path.basename(filename))
    if match:
        return {
            "ycsb_type": match.group(1),      # update
            "client_ip": match.group(2),      # 192.168.6.6
            "thread_num": match.group(3),     # 32
            "depth": match.group(4),          # 2
            "threshold": match.group(5),      # 50000
            "core_num": match.group(6),       # 1
            "timestamp": match.group(7)       # 2025-04-02 11:46:06.366294
        }
    return None

def aggregate_metrics():
"""dir, config , """
    # store all parsed results
    all_files = {}
    
    # iterate each client dir
    for client_ip in CLIENT_IPS:
        directory = os.path.join(LOG_DIR_BASE, client_ip, EXP_PREFIX)
        if not os.path.exists(directory):
            print(f"dir {directory} not found, skip")
            continue
        
        # scan dir for log files
        pattern = os.path.join(directory, f"latency_{exp_type}_zipfian_*.log")
        log_files = glob(pattern)
        
        for log_file in log_files:
            parsed = parse_filename(log_file)
            if parsed:
                config_key = f"ycsb_{parsed['ycsb_type']}_thread_{parsed['thread_num']}_depth_{parsed['depth']}_threshold_{parsed['threshold']}_core_{parsed['core_num']}_timestamp_{parsed['timestamp']}"
                if config_key not in all_files:
                    all_files[config_key] = {}
                all_files[config_key][parsed['client_ip']] = log_file
    
# config
    results = {}
    for config_key, files_dict in all_files.items():
        if len(files_dict) != 4:  # ensure 4 client files per config
print(f"config {config_key} file 4, skip")
            continue
        
        total_attempts = 0
        total_latency = 0.0
        valid_latency_count = 0
        
        for client_ip, file_path in files_dict.items():
            attempts, avg_latency = parse_file_content(file_path)
            
            # accumulate attempts
            if attempts is not None:
                total_attempts += attempts
            
            # accumulate latency
            if avg_latency is not None:
                total_latency += avg_latency
                valid_latency_count += 1
        
        # compute IOPS and avg latency
        iops = total_attempts / TIME if total_attempts > 0 else 0
        avg_latency = total_latency / valid_latency_count if valid_latency_count > 0 else 0
        
        results[config_key] = {
            "total_attempts": total_attempts,
            "iops": iops,
            "avg_latency": avg_latency,
            "valid_files": len(files_dict)
        }
    
    return results

def save_results(results, output_dir):
"""file"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for config_key, metrics in results.items():
        output_file = os.path.join(output_dir, f"metrics_{config_key}.txt")
        with open(output_file, 'w') as f:
            f.write(f"Total Attempts: {metrics['total_attempts']}\n")
            f.write(f"IOPS: {metrics['iops']:.3f}\n")
            f.write(f"Average Latency: {metrics['avg_latency']:.3f} us\n")
            f.write(f"Valid Files: {metrics['valid_files']}\n")
        print(f"results saved to  {output_file}")

def main():
    # output directory
    output_directory = f"./extract_result/{EXP_PREFIX}/"
    results = aggregate_metrics()
    save_results(results, output_directory)

if __name__ == "__main__":
    main()