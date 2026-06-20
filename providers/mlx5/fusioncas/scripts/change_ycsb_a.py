import os
from glob import glob

# define client IPs and directory structure
CLIENT_IPS = ["192.168.6.3", "192.168.6.4", "192.168.6.5", "192.168.6.6"]
EXP_PREFIX = "0416errorbar"
LOG_DIR_BASE = "../exp_log"
dist = "zipfian"

def rename_ycsb_a_files():
    """scan dirs, rename 'ycsb_a' to 'u50r50' in filenames"""
    renamed_count = 0
    
    # iterate over each client IP directory
    for client_ip in CLIENT_IPS:
        directory = os.path.join(LOG_DIR_BASE, client_ip, EXP_PREFIX)
        
        # check if directory exists
        if not os.path.exists(directory):
            print(f"directory  {directory}  not found, skipping")
            continue
        
        # glob for matching files
        pattern = os.path.join(directory, f"latency_enable_{dist}_ycsb_a_*.log")
        ycsb_a_files = glob(pattern)
        
        if not ycsb_a_files:
            print(f"in dir  {directory} : no 'ycsb_a' files found")
            continue
        
        # iterate and rename s
        for old_file_path in ycsb_a_files:
            old_filename = os.path.basename(old_file_path)
            directory_path = os.path.dirname(old_file_path)
            
            # check if filename contains 'ycsb_a'
            if "ycsb_a" in old_filename:
                # replace 'ycsb_a' with 'u50r50'
                new_filename = old_filename.replace("ycsb_a", "u50r50")
                new_file_path = os.path.join(directory_path, new_filename)
                
                # rename 
                try:
                    os.rename(old_file_path, new_file_path)
                    print(f"renamed : {old_filename} -> {new_filename}")
                    renamed_count += 1
                except OSError as e:
                    print(f"rename  {old_filename}  failed: {e}")
            else:
                print(f"skip  {old_filename}: : 'ycsb_a' not found")
    
    # print summary
    if renamed_count > 0:
        print(f"\nrename complete, processed  {renamed_count}  files")
    else:
        print("\nno files to rename found")

def main():
    rename_ycsb_a_files()

if __name__ == "__main__":
    main()