# Information Systems, Analysis and Design Class Project

Author: Nikolaos Kormpakis
AM: 03110785
Year: 2025-2026

# Description

This repository contains all source code to deploy a lab for SQL2.

* Ansible playbooks and roles for:
  * Deployment of K3s
  * Installation of Trino, PostgreSQL, MongoDB, Elasticsearch, ECK Operator using Helm
  * Configuration
* Scripts and Kubernetes resources for bulk data generation and loading
* SQL Queries for Benchmarking

# Prerequisites

## Install Tools

```
mise trust
mise install
```

Install Trino CLI from [here](https://trino.io/docs/current/client/cli.html).

## Configure Python

```
uv venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt
ansible-galaxy collection install -r requirements.yml
```

# Deployment

## Deploy VMs @ ~okeanos-knossos

Use `kamaki` or ~okeanos-knossos' UI for VM creation.
4x VMs are recommended:
* 1x k3s control plane
* 3x k3s workers

## Configure Inventory

Add host information to `./ansible/inventory/hosts.yml`.

## Deploy

_Inside venv_

```
ansible-playbook site.yml
```

## Get kubeconfig

```
ssh user@k3s-cp-node 'sudo cat /etc/rancher/k3s/k3s.yaml' > ./tmp/k3s-kubeconfig.yaml
export KUBECONFIG=./tmp/k3s-kubeconfig.yaml

kubectl get nodes
```

## Load Data

```
# Modify ./data-loader/secret-es-creds.yaml with ES' credentials
kubectl -n trino-benchmark apply -f secret-es-creds.yaml
kubectl -n trino-benchmark create configmap trino-es-loader --from-file=./data-loader/loader.py --dry-run=client -o yaml | kubectl apply -f -
kubectl -n trino-benchmark apply -f job-loader-psql-mongo.yaml
kubectl -n trino-benchmark apply -f job-loader-es.yaml
```

# Benchmarking

## Scale Trino Workers

Repeat for N=1 and N=5

```
kubectl -n trino-benchmark scale deploy trino-worker --replicas 1
```
## Run Queries

In separate terminal session:

```
kubectl -n trino-benchmark port-forward svc/trino 8080:8080
```

Connect to trino:

```
trino --server http://localhost:8080
```

Execute queries from `./queries`
