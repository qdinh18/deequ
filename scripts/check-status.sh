#!/bin/bash
echo " DataHub Status Check"
echo "======================="

echo " Namespace:"
kubectl get ns datahub

echo -e "\n️ Pods:"
kubectl get pods -n datahub -o wide

echo -e "\n Services:"
kubectl get svc -n datahub

echo -e "\n Storage:"
kubectl get pvc -n datahub

echo -e "\n Deployments:"
kubectl get deployments -n datahub

echo -e "\n StatefulSets:"
kubectl get statefulsets -n datahub

echo -e "\n Endpoints:"
echo "DataHub UI: http://$(minikube ip):30002"

