import paramiko
import time
import subprocess
import json
import os
import logging
import threading
from typing import Union
import shutil
import datetime
from scp import SCPClient


# server and client config
execute_file = "faa_fusa"
SERVER_COMMAND = f'../build/test/{execute_file} server'  # server program path
# SERVER_COMMAND = './test/test_rdma'
THREAD_NUMS = [32] #, 28, 24, 20, 16, 12, 8]  # client thread count
THRESHOLDS = [4000]#] 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000]          # client threshold
STRIDES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]
DEPTHS = [4]
CLIENT_IPS = ["192.168.6.4", "192.168.6.3", "192.168.6.5", "192.168.6.6"]  # client IP address array
CLIENT_PORTS = [2222, 2222, 2222, 2222]  # client port array
CLIENT_USERS = ["dell", "dell", "dell", "dell"]  # client username array
CLIENT_PASSWORDS = ["204msjcllm", "204msjcllm@1OI.c0M", "204msjcllm", "204msjcllm"]  # client password array
YCSB_TYPES = ["update"] #, "u70r30", "ycsb_a", "u30r70"] #["u90r10", "u80r20","u70r30", "u60r40", 
# YCSB_TYPES = ["u60r40"] #, "u90r10", "u80r20","u70r30", "u60r40", "ycsb_a", "u40r60", "u30r70", "u20r80", "u10r90", "u0r100"] #["update", "ycsb_a", "u0r100"]
# CONFIG_PATH = "../config/test_rdma.json"  # config file path
EXP_TYPES = ["enable"]
exp_prefix = "faa_fusa"
DIST_TYPES = ["zipfian"]


# configure logging
LOG_FILE = "experiment_log.txt"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(message)s')

# main function
def start_server_rdma():
    logging.info(f"start server-side  ./test/{execute_file}...")
    try:
        # start server process
        server_process = subprocess.Popen(SERVER_COMMAND, shell=True)
        # wait for process to start
        time.sleep(5)  # wait 5  seconds, ensure  process starts 
        return server_process
    except Exception as e:
        logging.error(f"start server-side  ./test/{execute_file} : failed:  {e}")
        return None

# start client-side test_rdma via SSH
def start_client_rdma(thread_num, depth, stride_value, log_file, client_ip, client_port, client_user, client_pass):
    logging.info(f"via  SSH start client-side  ./test/{execute_file} {thread_num} {depth} {stride_value}...")

    # get current time
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # create SSH client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # auto-accept host key
    ssh_client.connect(client_ip, username=client_user, password=client_pass, port=client_port)  # connect client

    if client_ip == "192.168.6.9":
        command = f'cd /home/sqs/rdma-core/providers/mlx5/fusioncas/build && make -j && LD_PRELOAD="./libmlx5.so ./libibverbs.so" ./test/{execute_file} client {thread_num} {depth}'
    else:
        command = f'cd /home/dell/sqs/rdma-core/providers/mlx5/fusioncas/build && make -j && LD_PRELOAD="./libmlx5.so ./libibverbs.so" ./test/{execute_file} client {thread_num} {depth}'

    # create log file on client,  write start time
    with open(log_file, 'w', encoding='utf-8') as log:
        log.write(f"experiment start time: {start_time}\n")  # log experiment start
        log.write("=" * 60 + "\n")

    # execute command log output
    with open(log_file, 'a', encoding='utf-8') as log:  # append mode
        stdin, stdout, stderr = ssh_client.exec_command(command)
        log.write(stdout.read().decode())
        log.write(stderr.read().decode())

    # get current time (experiment end)
    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, 'a', encoding='utf-8') as log:  # append mode
        log.write("=" * 60 + "\n")
        log.write(f"experiment end time: {end_time}\n")  # log experiment end

    return ssh_client


# stop server  test_rdma
def kill_server_rdma():
    logging.info(f"stop server-side  ./test/{execute_file}...\n\n")
    try:
        # force kill via pkill test_rdma  process
        subprocess.run(f"pkill -9 {execute_file}", shell=True, check=True)
        logging.info(f"terminated  {execute_file}  process.")
    except subprocess.CalledProcessError as e:
        logging.error(f"kill  {execute_file}  process: failed:  {e}")

def start_agent_server(threshold, agent_server_log, exp_type):
    # """start local agent server"""
    logging.info(f"start agent server (threshold={threshold})...")
    try:
        cmd = f"../build/agent/test_agent server {threshold} {exp_type}"
        server_proc = subprocess.Popen(cmd.split(), 
                                      stdout=open(agent_server_log, 'w'),
                                      stderr=subprocess.STDOUT)
        time.sleep(3)  # wait for server init
        return server_proc
    except Exception as e:
        logging.error(f"start agent server : failed:  {str(e)}")
        return None

def start_agent_client(threshold, log_file, client_ip, client_port, client_user, client_pass):
    logging.info(f"start  {client_ip}  agent client...")
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(client_ip, port=client_port, username=client_user, password=client_pass, timeout=10)
        
        # remote log file path
        remote_log = f"agent_client_{threshold}.log"
        
        # run in background command,  redirect logs
        if client_ip == "192.168.6.9":
            command = f"cd /home/sqs/rdma-core/providers/mlx5/fusioncas/build && nohup ./agent/test_agent client > {remote_log} 2>&1 &"
        else:
            command = f"cd /home/dell/sqs/rdma-core/providers/mlx5/fusioncas/build && nohup ./agent/test_agent client > {remote_log} 2>&1 &"
        ssh.exec_command(command, timeout=5)
        
        # close connection (command running in bg, keep connection not needed)
        ssh.close()
        
        # copy remote logs locally (optional)
        # with SCPClient(ssh.get_transport()) as scp:
        #     scp.get(remote_log, log_file)
            
        return None  # no SSH object to return
    except Exception as e:
        logging.error(f"start agent client : failed:  {str(e)}")
        return None


def scp_config(client_ip, client_port, user, password, local_path, remote_path):
    # """simplified SCP transfer"""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(client_ip, port=client_port, username=user, password=password, timeout=10)
        
        with SCPClient(ssh.get_transport()) as scp:
            # unified remote path (adjust as needed)
            scp.put(local_path, remote_path)
            logging.info(f"transferred to   {client_ip}  succeeded")
        ssh.close()
    except Exception as e:
        logging.error(f"transferred to   {client_ip} : failed:  {str(e)}")

def modify_ycsb_path(config_path: str, 
                    new_path: str,
                    backup: bool = True) -> Union[bool, Exception]:
    # """
    # modify ycsb_path field in JSON config
    
    # :param config_path: JSON config file path
    # :param new_path: new ycsb path
    # :param backup: whether to create backup
    # :return:  succeeded returns True, returns Exception on failure
    # """
    try:
        # validate parameters
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"config file  {config_path}  not found")
            
        if not isinstance(new_path, str) or len(new_path) < 3:
            raise ValueError("new path must be a valid string")

        # create backup (optional)
        if backup:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{config_path}.bak_{timestamp}"
            shutil.copyfile(config_path, backup_path)
            logging.info(f"config backup created: {backup_path}")

        # read and modify config
        for i in range(len(CLIENT_IPS)):
            with open(config_path, 'r+', encoding='utf-8') as f:
                # load JSON data
                config = json.load(f)
                
                # check data structure integrity
                if 'compute_node' not in config:
                    raise KeyError("JSON missing compute_node field")
                    
                compute_node = config['compute_node']
                if not isinstance(compute_node, dict):
                    raise TypeError("compute_node type error, expected dict")
                
                # modify target field
                compute_node['ycsb_path'] = new_path
                compute_node['machine_id'] = i
                logging.info(f"changed ycsb_path from  {compute_node.get('ycsb_path')}  to  {new_path}")

                # write back to file
                f.seek(0)  # seek to file start
                json.dump(config, f, indent=2, ensure_ascii=False)
                f.truncate()  # truncate remaining
            if CLIENT_IPS[i] == "192.168.6.9":
                remote_path_ = f"/home/sqs/rdma-core/providers/mlx5/fusioncas/config/test_rdma.json"
            else:
                remote_path_ = f"/home/dell/sqs/rdma-core/providers/mlx5/fusioncas/config/test_rdma.json"
            scp_config(
                client_ip=CLIENT_IPS[i],
                client_port=CLIENT_PORTS[i],
                user=CLIENT_USERS[i],
                password=CLIENT_PASSWORDS[i],
                local_path=config_path,
                remote_path = remote_path_
            )
            
        return True

    except Exception as e:
        logging.error(f"config modification : failed:  {str(e)}")
        return e

def modify_agent_config(config_path: str, 
                    new_path: str,
                    backup: bool = True) -> Union[bool, Exception]:
    # """
    # modify ycsb_path field in JSON config
    
    # :param config_path: JSON config file path
    # :param new_path: new ycsb path
    # :param backup: whether to create backup
    # :return:  succeeded returns True, returns Exception on failure
    # """
    try:
        # validate parameters
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"config file  {config_path}  not found")

        # create backup (optional)
        if backup:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{config_path}.bak_{timestamp}"
            shutil.copyfile(config_path, backup_path)
            logging.info(f"config backup created: {backup_path}")

        # read and modify config
        for i in range(len(CLIENT_IPS)):
            with open(config_path, 'r+', encoding='utf-8') as f:
                # load JSON data
                config = json.load(f)
                
                # check data structure integrity
                if 'compute_node' not in config:
                    raise KeyError("JSON missing compute_node field")
                    
                compute_node = config['compute_node']
                if not isinstance(compute_node, dict):
                    raise TypeError("compute_node type error, expected dict")
                
                # modify target field
                compute_node['machine_id'] = i
                logging.info(f"changed machine_id from {compute_node.get('machine_id')}  to  {i}")

                # write back to file
                f.seek(0)  # seek to file start
                json.dump(config, f, indent=2, ensure_ascii=False)
                f.truncate()  # truncate remaining
            if CLIENT_IPS[i] == "192.168.6.9":
                remote_path_ = f"/home/sqs/rdma-core/providers/mlx5/fusioncas/config/agent.json"
            else:
                remote_path_ = f"/home/dell/sqs/rdma-core/providers/mlx5/fusioncas/config/agent.json"
            scp_config(
                client_ip=CLIENT_IPS[i],
                client_port=CLIENT_PORTS[i],
                user=CLIENT_USERS[i],
                password=CLIENT_PASSWORDS[i],
                local_path=config_path,
                remote_path=remote_path_
            )
            
        return True

    except Exception as e:
        logging.error(f"config modification : failed:  {str(e)}")
        return e


def stop_agent_clients():
    """terminate all clients via SSH agent client  process"""
    for i in range(len(CLIENT_IPS)):
        client_ip = CLIENT_IPS[i]
        client_port = CLIENT_PORTS[i]
        client_user = CLIENT_USERS[i]
        client_pass = CLIENT_PASSWORDS[i]
        
        try:
            # create SSH connection
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(client_ip, port=client_port, 
                       username=client_user, password=client_pass,
                       timeout=10)
            
            # terminate agent client by name and threshold
            command = f"pkill -f 'test_agent'"  # exact match with threshold parameter
            stdin, stdout, stderr = ssh.exec_command(command)
            
            # check command result
            exit_status = stdout.channel.recv_exit_status()
            if exit_status == 0:
                logging.info(f"terminated  {client_ip}  agent client")
            else:
# pkill returns 1 when process not found (expected)
                logging.debug(f"{client_ip} no running agent client process")
            
            ssh.close()
        except Exception as e:
            logging.error(f"kill  {client_ip}  agent client : failed:  {str(e)}")

def main():
    for distribution_type in DIST_TYPES:
        ycsb_prefix = f"/home/dell/sqs/ycsb_output/1-node/{distribution_type}/a/"
    # ycsb_prefix = "/home/dell/sqs/ycsb_output/1-node/zipfian/a/"
    # start agent server and client first
        for exp_type in EXP_TYPES:
            for ycsb_type in YCSB_TYPES:
                ycsb_path = ycsb_prefix + ycsb_type
                modify_ycsb_path("../config/test_rdma.json", ycsb_path, False)
                # ycsb_name = os.path.basename(ycsb_path.rstrip('/'))  # ensure path doesn't end with slash
                modify_agent_config("../config/agent.json", " ", False)
                for thread_num in THREAD_NUMS:
                    for depth in DEPTHS:
                        for threshold in THRESHOLDS:
                            logging.info(f"starting execution: :{exp_prefix}/latency_{exp_type}_{distribution_type}_{ycsb_type}_{thread_num}_{depth}_{threshold}")
                            # 1. start agent server
                            agent_server_log = f"../data/agent/{exp_prefix}/latency_{exp_type}_agent_server_{distribution_type}_{ycsb_type}_{thread_num}_{depth}_{threshold}.log"
                            agent_server = start_agent_server(threshold, agent_server_log, exp_type)
                            
                            # 2. start all agent clients
                            agent_clients = []
                            for i in range(len(CLIENT_IPS)):
                                log_file = f"../data/agent/{exp_prefix}/latency_{exp_type}_agent_client_{distribution_type}_{ycsb_type}_{CLIENT_IPS[i]}_{thread_num}_{depth}_{threshold}.log"
                                client = start_agent_client(
                                    threshold=threshold,
                                    log_file=log_file,
                                    client_ip=CLIENT_IPS[i],
                                    client_port=CLIENT_PORTS[i],
                                    client_user=CLIENT_USERS[i],
                                    client_pass=CLIENT_PASSWORDS[i]
                                )
                                if client:
                                    agent_clients.append(client)
                            time.sleep(10)  # wait for agent init
                            # iterate over thread and depth arrays, iterate over all combinations

                            # start server -side  {execute_file},  retry until success
                            server_process = None
                            retries = 0
                            while server_process is None and retries < 3:  # max retries: 3
                                server_process = start_server_rdma()
                                if server_process is None:
                                    logging.error(f"startup failed, wait  10 s before retry... (attempt  {retries + 1})")
time.sleep(10) # retry after 10s if startup fails
                                    retries += 1
                            
                            if server_process is None:
                                logging.error(f"could not start server-side   {execute_file}, exit")
                                return  # exit after 3 failed startup attempts

                            time.sleep(10)  # wait 15s for full server startup
                
                            threads = []
                            for i in range(len(CLIENT_IPS)):
                                client_ip = CLIENT_IPS[i]
                                client_port = CLIENT_PORTS[i]
                                client_user = CLIENT_USERS[i]
                                client_pass = CLIENT_PASSWORDS[i]
                                # start multiple clients
                                logging.info(f"start  {CLIENT_IPS[i]} -side  {execute_file}, threads: {thread_num}, depth: {depth}...")

                                # create log filename, format:  thread_num_depth_stride_clientX.log
                                log_file = f"../data/{CLIENT_IPS[i]}/{exp_prefix}/latency_{exp_type}_{distribution_type}_{ycsb_type}_{CLIENT_IPS[i]}_{thread_num}_{depth}_{threshold}_client{i+1}.log"

                                # start client-side  test_rdma,  log output to file
                                thread = threading.Thread(target=start_client_rdma, args=(thread_num, depth, 0, log_file, client_ip, client_port, client_user, client_pass))
                                threads.append(thread)
                                thread.start()

                            # wait for all client threads to finish
                            for thread in threads:
                                thread.join()

                            time.sleep(5)
                            # stop server  test_rdma
                            kill_server_rdma()
                            stop_agent_clients()
                            time.sleep(10)
                            # stop server process
                            agent_server.terminate()

if __name__ == "__main__":
    main()
