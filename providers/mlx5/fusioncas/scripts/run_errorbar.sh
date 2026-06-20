#!/bin/bash

#
if [ $# -ne 1 ]; then
echo ": $0 <>"
echo ": $0 5"
    exit 1
fi

#
n=$1

#
if ! [[ "$n" =~ ^[0-9]+$ ]] || [ "$n" -le 0 ]; then
echo ": "
    exit 1
fi

# n
for ((i=1; i<=n; i++)); do
echo " $i ..."

#
    ./kill.sh
    if [ $? -ne 0 ]; then
echo ": ./kill.sh , "
    fi

    python3 ./run_cas_retry.py
    if [ $? -ne 0 ]; then
echo ": python3 ./run_cas_retry.py , "
    fi

    ./kill.sh
    if [ $? -ne 0 ]; then
echo ": ./kill.sh , "
    fi

    python3 ./run_cas_retry_4workloads.py
    if [ $? -ne 0 ]; then
echo ": python3 ./run_cas_retry_4workloads.py , "
    fi

echo " $i "
    echo "------------------------"
done

echo " $n "