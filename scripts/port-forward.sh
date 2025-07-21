#!/bin/bash
# Port forward các services để test local

echo " Setting up port forwarding..."

# DataHub Frontend
kubectl port-forward -n datahub svc/datahub-frontend 9002:9002 &
echo "DataHub UI: http://localhost:9002"

# DataHub GMS API
kubectl port-forward -n datahub svc/datahub-gms 8080:8080 &
echo "DataHub API: http://localhost:8080"

# MySQL (for debugging)
kubectl port-forward -n datahub svc/mysql 3306:3306 &
echo "MySQL: localhost:3306"

# Elasticsearch
kubectl port-forward -n datahub svc/elasticsearch 9200:9200 &
echo "Elasticsearch: http://localhost:9200"

echo "✅ Port forwarding active. Press Ctrl+C to stop."
wait
