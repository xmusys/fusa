#!/bin/bash

# local path config
prefix="/home/dell/sqs"
file_name_list=("u90r10" "u80r20" "u70r30" "u60r40" "u40r60" "u30r70" "u20r80" "u10r90")  # filename list
source_dir="$prefix/ycsb_output/1-node/zipfian/a"

# remote machine config
CLIENT_IPS=("192.168.6.4" "192.168.6.3" "192.168.6.5" "192.168.6.9")
CLIENT_PORTS=("2222" "2222" "2222" "22222")
CLIENT_USERS=("dell" "dell" "dell" "sqs")
CLIENT_PASSWORDS=("204msjcllm" "204msjcllm@1OI.c0M" "204msjcllm" "204msjcllm")

# target directory
remote_base_dir="/home/dell/sqs/ycsb_output/1-node/zipfian/a"

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
    remote_dir="$remote_base_dir/"  # target directory

    echo -e "\n===== Processing ${ip}:${port} ====="

    # create dirs for all except node9 (192.168.6.9, i!=3)
    if [ "$i" -ne 3 ]; then
        export SSHPASS="$password"
        sshpass -e ssh -o StrictHostKeyChecking=no -p "$port" "$user@$ip" "mkdir -p $remote_dir"
        if [ $? -eq 0 ]; then
            echo "Remote dir $remote_dir ready"
        else
            echo "[ERROR] Cannot create remote dir $remote_dir"
            continue
        fi
    else
        echo "node9 (${ip}) skip dir creation, assuming it exists"
    fi

    # iterate and copy files
    for file_name in "${file_name_list[@]}"
    do
        source_file="$source_dir/$file_name"
        
        # check file existence
        if [ -f "$source_file" ]; then
            echo "Copying $file_name to $user@$ip:$remote_dir ..."
            export SSHPASS="$password"
            sshpass -e scp -P "$port" -o StrictHostKeyChecking=no "$source_file" "$user@$ip:$remote_dir/"
            if [ $? -eq 0 ]; then
                echo "$file_name copied successfully"
            else
                echo "[ERROR] $file_name copy failed"
            fi
        else
            echo "File $source_file not found, skipping"
        fi
    done

    echo "File copy done on ${ip}"
done

echo -e "\n===== All machines processed ====="