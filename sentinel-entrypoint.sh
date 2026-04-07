#!/bin/sh
sleep 2

# Resolve all master IPs
ORDER_IP=$(getent hosts redis-order | awk '{print $1}')
STOCK_IP=$(getent hosts redis-stock | awk '{print $1}')
PAYMENT_IP=$(getent hosts redis-payment | awk '{print $1}')
ORCH_IP=$(getent hosts redis-orchestrator | awk '{print $1}')

# Replace hostnames with IPs in config
sed \
  -e "s/redis-order/$ORDER_IP/g" \
  -e "s/redis-stock/$STOCK_IP/g" \
  -e "s/redis-payment/$PAYMENT_IP/g" \
  -e "s/redis-orchestrator/$ORCH_IP/g" \
  /etc/redis/sentinel-ro.conf > /tmp/sentinel.conf

exec redis-sentinel /tmp/sentinel.conf