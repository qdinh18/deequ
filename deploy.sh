#!/bin/bash
set -e

echo " Deploying DataHub on Kubernetes..."

# Tạo namespace trước
echo " Creating namespace..."
kubectl apply -f 00-namespace/

# Config và secrets
echo "⚙️ Applying configurations..."
kubectl apply -f 01-config/

# Storage classes
echo " Setting up storage..."
kubectl apply -f 02-storage/

# Infrastructure services
echo "️ Deploying infrastructure..."
kubectl apply -f 03-infrastructure/zookeeper/
kubectl apply -f 03-infrastructure/kafka/

# Wait for infrastructure
echo "⏳ Waiting for infrastructure..."
kubectl wait --for=condition=ready pod -l app=zookeeper -n datahub --timeout=300s
kubectl wait --for=condition=ready pod -l app=kafka -n datahub --timeout=300s

# Databases
echo "️ Deploying databases..."
kubectl apply -f 04-databases/mysql/
kubectl apply -f 04-databases/elasticsearch/

# Wait for databases
echo "⏳ Waiting for databases..."
kubectl wait --for=condition=ready pod -l app=mysql -n datahub --timeout=600s
kubectl wait --for=condition=ready pod -l app=elasticsearch -n datahub --timeout=600s

# DataHub services
echo " Deploying DataHub..."
kubectl apply -f 05-datahub/gms/
kubectl apply -f 05-datahub/frontend/

# Wait for DataHub
echo "⏳ Waiting for DataHub..."
kubectl wait --for=condition=ready pod -l app=datahub-gms -n datahub --timeout=600s
kubectl wait --for=condition=ready pod -l app=datahub-frontend -n datahub --timeout=300s

echo "✅ DataHub deployed successfully!"
echo " Access DataHub at: http://$(minikube ip):30002"