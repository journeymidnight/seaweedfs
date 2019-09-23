#!/bin/bash

WEED_BIN=/work/seaweedfs/weed/weed

mkdir -p /seaweed_master
$WEED_BIN master -mdir=/seaweed_master \
	-ip=$SEAWEED_IP -ip.bind=0.0.0.0 -port=10001 \
	-volumePreallocate \
	-defaultReplication=000 \
	-volumeSizeLimitMB 1024 \
	&> /master.log &

sleep 5

mkdir -p /seaweed_volume
$WEED_BIN volume -dir=/seaweed_volume \
	-mserver=127.0.0.1:10001 \
	-ip=$SEAWEED_IP -ip.bind=0.0.0.0 -port=30001 \
	-lusfFileSizeGB 1 -max 3 \
	&> /volume.log