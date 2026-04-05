#!/bin/sh
# Wait for DNS to be ready
sleep 2
# Resolve redis-master IP and write config with IP instead of hostname
MASTER_IP=$(getent hosts redis-master | awk '{print $1}')
sed "s/redis-master/$MASTER_IP/" /etc/redis/sentinel-ro.conf > /tmp/sentinel.conf
exec redis-sentinel /tmp/sentinel.conf