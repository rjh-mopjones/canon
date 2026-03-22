# Canon Demo — Kubernetes deployment via minikube
#
# Usage:
#   make k8s-up      — full stack: start minikube, build images, deploy
#   make k8s-down    — tear down the canon namespace
#   make k8s-build   — rebuild all Docker images (run inside minikube docker-env)
#   make k8s-deploy  — apply Kustomize manifests
#   make k8s-status  — show pod/job/service status
#   make k8s-logs    — tail logs from all canon pods
#   make k8s-tunnel  — expose LoadBalancer services via minikube tunnel

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Minikube settings
MINIKUBE_CPUS ?= 4
MINIKUBE_MEMORY ?= 8192
MINIKUBE_DRIVER ?= docker

# Image names
IMAGES := fleet-service cargo-service navigation-service supply-service station-service gateway frontend
INIT_IMAGE := init-schema

# ── Targets ──────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: k8s-up
k8s-up: k8s-start k8s-build k8s-deploy ## Full stack: start minikube, build images, deploy
	@echo ""
	@echo "==> Canon demo is deploying. Run 'make k8s-status' to check progress."
	@echo "==> Run 'make k8s-tunnel' in a separate terminal to access the frontend."

.PHONY: k8s-start
k8s-start: ## Start minikube if not running
	@if ! minikube status --format '{{.Host}}' 2>/dev/null | grep -q Running; then \
		echo "==> Starting minikube (cpus=$(MINIKUBE_CPUS), memory=$(MINIKUBE_MEMORY)MB)..."; \
		minikube start --cpus=$(MINIKUBE_CPUS) --memory=$(MINIKUBE_MEMORY) --driver=$(MINIKUBE_DRIVER); \
	else \
		echo "==> minikube already running"; \
	fi

.PHONY: k8s-build
k8s-build: ## Build all Docker images in minikube's Docker daemon
	@echo "==> Configuring Docker to use minikube daemon..."
	@eval $$(minikube docker-env) && \
	echo "==> Building canon-builder base image..." && \
	docker build -t canon-builder -f Dockerfile.builder .. && \
	echo "==> Building service images..." && \
	$(foreach img,$(IMAGES), \
		echo "    building canon-demo/$(img)..." && \
		docker build -t canon-demo/$(img) -f $(img)/Dockerfile .. && \
	) \
	echo "==> Building init-schema image..." && \
	docker build -t canon-demo/init-schema init-schema/ && \
	echo "==> All images built."

.PHONY: k8s-deploy
k8s-deploy: ## Apply Kustomize manifests to minikube
	@echo "==> Applying Kustomize overlay (minikube)..."
	kubectl apply -k k8s/overlays/minikube/
	@echo "==> Waiting for init jobs to complete..."
	kubectl wait --for=condition=complete job/init-schema -n canon --timeout=120s 2>/dev/null || true
	kubectl wait --for=condition=complete job/init-kafka-topics -n canon --timeout=120s 2>/dev/null || true
	@echo "==> Manifests applied. Run 'make k8s-status' to check pod status."

.PHONY: k8s-down
k8s-down: ## Tear down the canon namespace
	@echo "==> Deleting canon namespace..."
	kubectl delete namespace canon --ignore-not-found
	@echo "==> Canon namespace deleted."

.PHONY: k8s-status
k8s-status: ## Show pod, job, and service status
	@echo "=== Pods ==="
	@kubectl get pods -n canon -o wide 2>/dev/null || echo "  (namespace not found)"
	@echo ""
	@echo "=== Jobs ==="
	@kubectl get jobs -n canon 2>/dev/null || echo "  (namespace not found)"
	@echo ""
	@echo "=== Services ==="
	@kubectl get svc -n canon 2>/dev/null || echo "  (namespace not found)"
	@echo ""
	@echo "=== StatefulSets ==="
	@kubectl get statefulsets -n canon 2>/dev/null || echo "  (namespace not found)"

.PHONY: k8s-logs
k8s-logs: ## Tail logs from all canon pods
	kubectl logs -n canon -l app.kubernetes.io/part-of=canon-demo --all-containers --follow --max-log-requests=20

.PHONY: k8s-tunnel
k8s-tunnel: ## Expose LoadBalancer services (frontend on localhost:80)
	@echo "==> Starting minikube tunnel (requires sudo)..."
	@echo "==> Frontend will be available at http://localhost:80"
	minikube tunnel

.PHONY: k8s-restart
k8s-restart: ## Restart all application deployments (keeps infra running)
	kubectl rollout restart deployment -n canon -l tier=app

.PHONY: k8s-clean
k8s-clean: k8s-down ## Full cleanup: delete namespace and stop minikube
	@echo "==> Stopping minikube..."
	minikube stop
