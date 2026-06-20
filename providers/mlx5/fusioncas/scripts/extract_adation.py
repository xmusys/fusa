import os
import re
from collections import defaultdict
from glob import glob

# define client IPs and directory structure
CLIENT_IPS = ["192.168.6.3", "192.168.6.4", "192.168.6.5", "192.168.6.6"]
EXP_PREFIX = "adaption"
LOG_DIR_BASE = "../exp_log"

def parse_file_content(file_path):
    """parse file, extract requests per second"""
    requests_per_second = defaultdict(int)
    try:
        with open(file_path, 'r') as f:
            content = f.read()
# regex match "second X Y"
            matches = re.findall(r"second (\d+) (\d+)", content)
            for second, requests in matches:
                second = int(second)
                requests = int(requests)
                requests_per_second[second] = requests
    except FileNotFoundError:
        print(f"file {file_path} not found")
    except Exception as e:
print(f"file {file_path} error: {e}")
    return requests_per_second

def parse_filename(filename):
    """parse filename, extract config and timestamp"""
    pattern = r"latency_enable_zipfian_adaption_(\d+\.\d+\.\d+\.\d+)_(\d+)_(\d+)_(\d+)_client\d+_(.+)\.log"
    match = re.match(pattern, os.path.basename(filename))
    if match:
        return {
            "client_ip": match.group(1),      # 192.168.6.4
            "thread_num": match.group(2),     # 32
            "depth": match.group(3),          # 2
            "threshold": match.group(4),      # 50000
            "timestamp": match.group(5)       # 2025-04-02 11:12:04.714939
        }
    return None

def aggregate_requests():
    """scan all dirs, group by timestamp and config, accumulate requests"""
    # store all parsed results
    all_files = {}
    
    # iterate each client dir
    for client_ip in CLIENT_IPS:
        directory = os.path.join(LOG_DIR_BASE, client_ip, EXP_PREFIX)
        if not os.path.exists(directory):
            print(f"dir {directory} not found, skip")
            continue
        
        # scan dir for log files
        pattern = os.path.join(directory, "latency_enable_zipfian_adaption_*.log")
        log_files = glob(pattern)
        
        for log_file in log_files:
            parsed = parse_filename(log_file)
            if parsed:
                config_key = f"thread_{parsed['thread_num']}_depth_{parsed['depth']}_threshold_{parsed['threshold']}_timestamp_{parsed['timestamp']}"
                if config_key not in all_files:
                    all_files[config_key] = {}
                all_files[config_key][parsed['client_ip']] = log_file
    
    # accumulate requests by config
    results = {}
    for config_key, files_dict in all_files.items():
        if len(files_dict) != 4:  # ensure 4 client files per config
print(f"config {config_key} file 4, skip")
            continue
        
        total_requests = defaultdict(int)
        for client_ip, file_path in files_dict.items():
            requests = parse_file_content(file_path)
            for second, count in requests.items():
                total_requests[second] += count
        
        results[config_key] = total_requests
    
    return results

def save_results(results, output_dir):
    """save accumulated results to file"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for config_key, total_requests in results.items():
        output_file = os.path.join(output_dir, f"total_requests_{config_key}.txt")
        with open(output_file, 'w') as f:
            for second in sorted(total_requests.keys()):
                f.write(f"second {second} {total_requests[second]}\n")
        print(f"results saved to  {output_file}")

def main():
    # output directory
    output_directory = f"./extract_result/{EXP_PREFIX}/"
    results = aggregate_requests()
    save_results(results, output_directory)

if __name__ == "__main__":
    main()