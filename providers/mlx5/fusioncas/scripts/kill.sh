#!/bin/bash

# machine config (IP, port, user, password)
CLIENT_IPS=("192.168.6.4" "192.168.6.3" "192.168.6.5" "192.168.6.6")
CLIENT_PORTS=("2222" "2222" "2222" "2222")
CLIENT_USERS=("dell" "dell" "dell" "dell")
CLIENT_PASSWORDS=("204msjcllm" "204msjcllm@1OI.c0M" "204msjcllm" "204msjcllm")

# programs to kill
process_list=("fusa_rpc_agent" "test_agent" "test_ycsb" "test_stride" "test_latency" "ycsb_exp" "test_herd" "test_static" "test_faa" "test_herd_faa" "faa_stride" "faa_fusa" "cas_retry" "test_adaption" "hashtable_bench" "sequencer")

# check if sshpass is installed
if ! command -v sshpass &> /dev/null; then
    echo "Installing sshpass..."
    sudo apt-get install -y sshpass > /dev/null || { echo "Installation failed, please install sshpass manually"; exit 1; }
fi

# iterate over all machines
for i in {0..3}; do
    ip="${CLIENT_IPS[$i]}"
    port="${CLIENT_PORTS[$i]}"
    user="${CLIENT_USERS[$i]}"
    password="${CLIENT_PASSWORDS[$i]}"

    echo -e "\n===== Processing ${ip}:${port} ====="
    
    # build remote command, iterate over program list
    cmd=""
    for process in "${process_list[@]}"; do
        cmd+="pkill -9 $process && echo '[OK] $process terminated' || echo '[WARN] $process not found'; "
    done

    # execute remote command via sshpass
    export SSHPASS="$password"
    if sshpass -e ssh -o StrictHostKeyChecking=no -p "$port" "${user}@${ip}" "$cmd"
    then
        echo "Completed process cleanup on ${ip}"
    else
        echo "[ERROR] Connection to ${ip} failed"
    fi
done

echo -e "\n===== All machines processed ===