#!/usr/bin/env bash

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm install -f helm-config/redis-helm-values.yaml redis bitnami/redis
REDIS_PASSWORD=$(kubectl get secret --namespace default redis -o jsonpath="{.data.redis-password}" | base64 -d)
kubectl run --namespace default redis-client --restart='Never'  --env REDIS_PASSWORD="$REDIS_PASSWORD"  --image registry-1.docker.io/bitnami/redis:latest --command -- sleep infinity