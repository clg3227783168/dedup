#!/bin/bash

# 清理所有镜像
sudo ctr image rm $(sudo ctr image ls -q)
sudo docker image prune -a -f
sudo rm -rf /var/lib/containerd/io.containerd.snapshotter.v1.dedup/*
df -h

time sudo docker run --rm docker.io/godnf/tst-lazy-pull:latest




sudo ctr image rm $(sudo ctr image ls -q)
sudo docker image prune -a -f
sudo rm -rf /var/lib/containerd/io.containerd.snapshotter.v1.dedup/*
df -h

# 启动资源监控
bash /home/clg/dedup/monitor_resource.sh dedup-snapshotter > ~/dedup/res/dedup_test.log &
MONITOR_PID=$!

time (
sudo ctr image pull --snapshotter=dedup docker.io/godnf/tst-lazy-pull:latest
sudo ctr run --snapshotter=dedup --rm docker.io/godnf/tst-lazy-pull:latest test-container
)
kill $MONITOR_PID