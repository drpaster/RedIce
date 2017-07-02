#! /bin/bash
for i in `seq 1 1000`;
do
  python3 ./redice-manager.py map create --name MyMap${i} --size 1 --blocks 4
  sleep 0.1
done
