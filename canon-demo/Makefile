# Canon Demo — Kubernetes deployment via minikube
#
# Usage:
#   make k8s-up      — full stack: start minikube, build images, deploy
#   make k8s-down    — tear down the canon namespace
#   make k8s-build   — cross-compile services locally, build slim Docker images
#   make k8s-deploy  — apply Kustomize manifests
#   make k8s-status  — show pod/job/service status
#   make k8s-logs    — tail logs from all canon pods
#   make k8s-test-e2e — run Playwright e2e smoke tests against running cluster
#   make k8s-tunnel  — expose LoadBalancer services via minikube tunnel

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Minikube settings
MINIKUBE_CPUS ?= 4
MINIKUBE_MEMORY ?= 8192
MINIKUBE_DRIVER ?= docker

# Cross-compilation target
CROSS_TARGET := aarch64-unknown-linux-musl

# Backend services (cross-compiled locally)
SERVICES := fleet-service cargo-service navigation-service supply-service station-service gateway

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
k8s-build: ## Cross-compile services and build Docker images
	@echo "==> Cross-compiling services for $(CROSS_TARGET)..."
	cargo build --release --target $(CROSS_TARGET) \
		$(foreach svc,$(SERVICES),-p $(svc))
	@echo "==> Building service Docker images..."
	@$(foreach svc,$(SERVICES), \
		echo "    building canon-demo/$(svc)..." && \
		docker build -t canon-demo/$(svc) -f $(svc)/Dockerfile ../. && \
	) true
	@echo "==> Building frontend image..."
	docker build -t canon-demo/frontend -f frontend/Dockerfile ../.
	@echo "==> Building init-schema image..."
	docker build -t canon-demo/init-schema init-schema/
	@echo "==> Loading images into minikube..."
	@$(foreach svc,$(SERVICES), \
		minikube image load canon-demo/$(svc) && \
	) true
	minikube image load canon-demo/frontend
	minikube image load canon-demo/init-schema
	@echo "==> All images built and loaded."

.PHONY: k8s-deploy
k8s-deploy: ## Apply Kustomize manifests to minikube
	@echo "==> Applying Kustomize overlay (minikube)..."
	kubectl apply -k k8s/overlays/minikube/
	@echo "==> Waiting for infrastructure pods..."
	kubectl wait --for=condition=ready pod -l tier=infra -n canon --timeout=300s
	@echo "==> Waiting for init jobs to complete..."
	kubectl wait --for=condition=complete job/init-schema -n canon --timeout=120s
	kubectl wait --for=condition=complete job/init-kafka-topics -n canon --timeout=120s
	@echo "==> Waiting for application pods..."
	kubectl wait --for=condition=ready pod -l tier=app -n canon --timeout=120s
	@echo "==> All pods ready. Run 'make k8s-status' to verify."

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

.PHONY: k8s-test-e2e
k8s-test-e2e: ## Run Playwright e2e smoke tests against running cluster
	@echo "==> Running e2e smoke tests (requires frontend on :3000, gateway on :8080)..."
	@npx playwright install chromium --with-deps 2>/dev/null || true
	@node e2e/test.js

.PHONY: k8s-test-supply-chain
k8s-test-supply-chain: ## Run full supply chain loop test (4 legs)
	@echo "==> Running supply chain loop test..."
	@node e2e/test-supply-chain.js

.PHONY: k8s-test-multi-tab
k8s-test-multi-tab: ## Run multi-tab session isolation test
	@echo "==> Running multi-tab isolation test..."
	@node e2e/test-multi-tab.js

.PHONY: k8s-test-stress
k8s-test-stress: ## Run stress test (3 tabs × 2 rounds by default)
	@echo "==> Running stress test..."
	@node e2e/test-stress.js

.PHONY: k8s-test-all
k8s-test-all: k8s-test-e2e k8s-test-supply-chain k8s-test-multi-tab ## Run all e2e tests

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
