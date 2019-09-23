#!/bin/bash

WEED_BIN=/work/seaweedfs/weed/weed

mkdir -p /seaweed_master
$WEED_BIN master -mdir=/seaweed_master \
	-ip="$SEAWEED_IP" -ip.bind=0.0.0.0 -port=10001 \
	-volumePreallocate \
	-defaultReplication=000 \
	-volumeSizeLimitMB 1024 \
	&> /master.log &

sleep 5

for i in {1..3}
do
  mkdir -p /seaweed_volume_$i
  $WEED_BIN volume -dir=/seaweed_volume_$i \
    -mserver=127.0.0.1:10001 \
    -ip="$SEAWEED_IP" -ip.bind=0.0.0.0 -port=3000$i \
    -lusfFileSizeGB 1 -max 1 \
    &> /volume.log &
done

while true;
do
  sleep 10000;
done
