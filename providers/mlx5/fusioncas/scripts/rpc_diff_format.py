import os
import re
import csv
from collections import defaultdict

# define column mapping from filename numbers
CONFIG_MAPPING = {
    "32": "128",
    "30": "120",
    "28": "112",
    "26": "104",
    "24": "96",
    "22": "88",
    "20": "80",
    "18": "72",
    "16": "64",
    "14": "56",
    "12": "48",
    "10": "40",
    "8": "32",
    "4": "16",
    "2": "8"
}

# define YCSB types
YCSB_TYPES = ["update", "u75r25", "u50r50", "u25r75"]#["u50r50", "u40r60", "u30r70", "u20r80", "u10r90"]

# define threshold list
THRESHOLDS = [0]

exp_prefix = "rpc_diff/fusa"

# init table data structure, YCSB ,
tables = defaultdict(lambda: defaultdict(lambda: {"throughput": "", "latency": "", "timestamp": ""}))
columns = ["128", "120", "112", "104", "96", "88", "80", "72", "64", "56", "48", "40", "32", "16", "8"]

def parse_file_content(file_path):
    """parse file, extract Total IOPS and Total Avg Latency, strip units"""
    throughput = ""
    latency = ""
    with open(file_path, 'r') as f:
        content = f.read()
        # extract Total IOPS and Total Avg Latency via regex, strip units
        iops_match = re.search(r"Total IOPS: (\d+\.\d+) M/s", content)
        latency_match = re.search(r"Total Avg Latency: (\d+\.\d+) us", content)
        if iops_match:
            throughput = iops_match.group(1)  # numeric value only
        if latency_match:
            latency = latency_match.group(1)  # numeric value only
    return throughput, latency

def process_directory(directory):
    """scan directory, process all matching files"""
    for filename in os.listdir(directory):
        if filename.startswith("latency_enable_uniform_") and filename.endswith(".txt"):
            parts = filename.split("_")
            if len(parts) >= 8:  # ensure enough fields including timestamp
                ycsb_type = parts[3]  # extract 4th field, e.g. "update"
                config_value = parts[4]  # extract 5th field, e.g. "32"
                threshold = parts[6]  # extract 7th field, e.g. "10000000"
                timestamp = "_".join(parts[7:])[:-4]  # extract timestamp, strip  ".txt", e.g. "2025-03-31 14:46:06.199446"
                try:
                    threshold = int(threshold)  # convert to int for comparison
                    if ycsb_type in YCSB_TYPES and threshold in THRESHOLDS:
                        mapped_column = CONFIG_MAPPING.get(config_value, config_value)  # map to table column
                        if mapped_column in columns:
                            key = f"{ycsb_type}_threshold_{threshold}"
                            file_path = os.path.join(directory, filename)
                            throughput, latency = parse_file_content(file_path)
                            tables[key][mapped_column]["throughput"] = throughput
                            tables[key][mapped_column]["latency"] = latency
                            tables[key][mapped_column]["timestamp"] = timestamp  # store timestamp
                except ValueError:
                    continue  # skip if threshold is not int

def save_to_csv(output_dir):
    """save results to CSV named by ycsb_type, threshold, timestamp"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # track timestamps per key (assume same timestamp per key)
    timestamp_map = {}
    for key in tables.keys():
        for col in columns:
            if tables[key][col]["timestamp"]:
                timestamp_map[key] = tables[key][col]["timestamp"]
                break  # get first non-empty timestamp

    for key in tables.keys():
        if key in timestamp_map:
            csv_filename = os.path.join(output_dir, f"{key}_{timestamp_map[key]}.csv")
            with open(csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                # write header: pair throughput and latency
                header = ["metric"]
                for col in columns:
                    header.append(f"{col}_throughput")
                    header.append(f"{col}_latency")
                writer.writerow(header)
                
                # write data rows: throughput and latency per column
                row = ["value"]
                for col in columns:
                    throughput = tables[key][col]["throughput"] or ""
                    latency = tables[key][col]["latency"] or ""
                    row.append(throughput)
                    row.append(latency)
                writer.writerow(row)

def main():
    # specify directory path
    input_directory = f"./extract_result/{exp_prefix}/"
    output_directory = f"./extract_result/{exp_prefix}/format/"
    process_directory(input_directory)
    save_to_csv(output_dir=output_directory)

if __name__ == "__main__":
    main()