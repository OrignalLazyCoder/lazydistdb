#!/bin/bash

echo $RANDOM

for shard in 127.0.0.1:8080; do
    for i in {1..1000}; do 
        curl "http://$shard/set?key=key-$i&value=value-$i"
    done
done