echo "timestamp,container,cpu,mem_usage,net_io" > stats.csv
while true; do
  ts=$(date +%s)
  docker stats --no-stream --format "${ts},{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}" >> stats.csv
  sleep 1
done