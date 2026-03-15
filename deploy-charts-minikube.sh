#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting minikube ==="
if minikube status --format='{{.Host}}' 2>/dev/null | grep -q Running; then
  echo "Minikube is already running, skipping start."
else
  minikube start --cpus=max --memory=23700
fi

echo "=== Enabling ingress addon ==="
minikube addons enable ingress

echo "=== Setting up Helm repos ==="
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

echo "=== Installing Redis with Sentinel ==="
helm upgrade --install -f helm-config/redis-helm-values.yaml redis bitnami/redis

echo "=== Waiting for Redis pods to be ready ==="
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis --timeout=300s

echo "=== Switching to minikube Docker daemon ==="
eval "$(minikube docker-env)"

echo "=== Building Docker images ==="
docker build -t order:latest ./order
docker build -t stock:latest ./stock
docker build -t user:latest ./payment

echo "=== Applying Kubernetes manifests ==="
kubectl apply -f k8s/

echo "=== Waiting for deployments to roll out ==="
#kubectl rollout restart deployment order-deployment stock-deployment user-deployment
kubectl rollout status deployment order-web stock-web user-web order-worker stock-worker user-worker --timeout=120s

echo "=== Waiting for ingress controller to be ready ==="
kubectl wait --namespace ingress-nginx --for=condition=ready pod -l app.kubernetes.io/component=controller --timeout=120s

echo ""
echo "=== Deployment complete! ==="
echo "Minikube IP: $(minikube ip)"
echo "To access services, run: minikube tunnel"
echo "Or use: $(minikube service ingress-nginx-controller -n ingress-nginx --url | head -1)/..."
