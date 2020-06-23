#!/bin/bash

echo "batch number starts:" $1
echo "batch number ends:" $2
for ((i = $1; i < $2; i++ )); do
    echo "day shift:" $i
    python3 producer_REDD_avro.py config.ini "$i" "$i" &
    sleep 30
done
