#!/bin/bash
# Xem logs của tất cả services

if [ -z "$1" ]; then
    echo "Usage: ./logs.sh <service-name>"
    echo "Available services: mysql, elasticsearch, kafka, zookeeper, datahub-gms, datahub-frontend"
    exit 1
fi

SERVICE=$1

case $SERVICE in
    "mysql")
        kubectl logs -f -n datahub statefulset/mysql
        ;;
    "elasticsearch")
        kubectl logs -f -n datahub statefulset/elasticsearch
        ;;
    "kafka")
        kubectl logs -f -n datahub deployment/kafka
        ;;
    "zookeeper")
        kubectl logs -f -n datahub deployment/zookeeper
        ;;
    "datahub-gms")
        kubectl logs -f -n datahub deployment/datahub-gms
        ;;
    "datahub-frontend")
        kubectl logs -f -n datahub deployment/datahub-frontend
        ;;
    *)
        echo "Unknown service: $SERVICE"
        exit 1
        ;;
esac
