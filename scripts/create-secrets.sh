#!/bin/bash
# Tạo secrets từ environment variables

echo " Creating secrets from environment..."

# Check required env vars
required_vars=("MYSQL_ROOT_PASSWORD" "MYSQL_USER" "MYSQL_PASSWORD" "MYSQL_DATABASE")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Environment variable $var is not set"
        exit 1
    fi
done

# Create secret
kubectl create secret generic datahub-secrets \
    --from-literal=mysql-root-password="$MYSQL_ROOT_PASSWORD" \
    --from-literal=mysql-username="$MYSQL_USER" \
    --from-literal=mysql-password="$MYSQL_PASSWORD" \
    --from-literal=mysql-database="$MYSQL_DATABASE" \
    --namespace=datahub \
    --dry-run=client -o yaml > 01-config/secrets.yaml

echo "✅ Secrets created successfully!"

