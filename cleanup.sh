#!/bin/bash

# Delete the DataHub release
helm delete datahub --namespace datahub

# Delete the namespace
kubectl delete namespace datahub
