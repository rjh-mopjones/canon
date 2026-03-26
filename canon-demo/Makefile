# Canon Demo — Kubernetes deployment via minikube (local) and GKE (production)
#
# Minikube targets:
#   make k8s-up      — full stack: start minikube, build images, deploy
#   make k8s-down    — tear down the canon namespace
#   make k8s-build   — cross-compile services locally, build slim Docker images
#   make k8s-deploy  — apply Kustomize manifests
#   make k8s-status  — show pod/job/service status
#   make k8s-logs    — tail logs from all canon pods
#   make k8s-test-e2e — run Playwright e2e smoke tests against running cluster
#   make k8s-tunnel  — expose LoadBalancer services via minikube tunnel
#
# GKE targets:
#   make gke-build-push     — cross-compile x86_64, build+push all images
#   make gke-deploy-prod    — apply prod overlay, wait for rollouts
#   make gke-deploy-staging — apply staging overlay, wait for rollouts
#   make gke-deploy         — deploy both prod and staging
#   make gke-status         — pods, services, ingress in all 3 namespaces
#   make gke-logs-prod      — tail prod app logs
#   make gke-logs-staging   — tail staging app logs
#   make gke-restart-prod   — rollout restart prod
#   make gke-restart-staging — rollout restart staging
#   make gke-monitoring-setup — create uptime checks + email alerts

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
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

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
		docker build -t canon-demo/$(svc) \
			--build-arg CROSS_TARGET=$(CROSS_TARGET) \
			-f $(svc)/Dockerfile ../. && \
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

# ── GKE targets ────────────────────────────────────────────────────────────────

GKE_REGISTRY := europe-west2-docker.pkg.dev/canon-demo-prod/canon
GKE_CROSS_TARGET := x86_64-unknown-linux-musl
GKE_DEPLOYS := fleet-service cargo-service navigation-service supply-service station-service gateway frontend

.PHONY: gke-build-push
gke-build-push: ## Cross-compile x86_64, build and push all images to Artifact Registry
	@echo "==> Cross-compiling services for $(GKE_CROSS_TARGET)..."
	cargo build --release --target $(GKE_CROSS_TARGET) \
		$(foreach svc,$(SERVICES),-p $(svc))
	@echo "==> Building and pushing service images..."
	@for svc in $(SERVICES); do \
		echo "    $$svc..."; \
		docker build --no-cache --build-arg CROSS_TARGET=$(GKE_CROSS_TARGET) \
			-t $(GKE_REGISTRY)/$$svc:latest \
			-f $$svc/Dockerfile ../.; \
		docker push $(GKE_REGISTRY)/$$svc:latest; \
	done
	@echo "==> Building and pushing frontend image..."
	docker build --no-cache -t $(GKE_REGISTRY)/frontend:latest \
		-f frontend/Dockerfile ../.
	docker push $(GKE_REGISTRY)/frontend:latest
	@echo "==> Building and pushing init-schema image..."
	docker build --no-cache -t $(GKE_REGISTRY)/init-schema:latest init-schema/
	docker push $(GKE_REGISTRY)/init-schema:latest
	@echo "==> All images pushed to $(GKE_REGISTRY)."

.PHONY: gke-deploy-prod
gke-deploy-prod: ## Apply prod overlay and wait for rollouts
	@echo "==> Deploying to canon-prod..."
	kubectl apply -k k8s/overlays/gke/prod/
	@for deploy in $(GKE_DEPLOYS); do \
		echo "    waiting for $$deploy..."; \
		kubectl rollout status deployment/$$deploy -n canon-prod --timeout=300s; \
	done
	@echo "==> canon-prod deployed."

.PHONY: gke-deploy-staging
gke-deploy-staging: ## Apply staging overlay and wait for rollouts
	@echo "==> Deploying to canon-staging..."
	kubectl apply -k k8s/overlays/gke/staging/
	@for deploy in $(GKE_DEPLOYS); do \
		echo "    waiting for $$deploy..."; \
		kubectl rollout status deployment/$$deploy -n canon-staging --timeout=300s; \
	done
	@echo "==> canon-staging deployed."

.PHONY: gke-deploy
gke-deploy: gke-deploy-prod gke-deploy-staging ## Deploy both prod and staging, then apply infra policies
	kubectl apply -k k8s/overlays/gke/infra-policies/ || true

.PHONY: gke-status
gke-status: ## Show pods, services, and ingress across all GKE namespaces
	@echo "=== canon-infra ==="
	@kubectl get pods,svc -n canon-infra 2>/dev/null || echo "  (namespace not found)"
	@echo ""
	@echo "=== canon-prod ==="
	@kubectl get pods,svc,ingress -n canon-prod 2>/dev/null || echo "  (namespace not found)"
	@echo ""
	@echo "=== canon-staging ==="
	@kubectl get pods,svc,ingress -n canon-staging 2>/dev/null || echo "  (namespace not found)"

.PHONY: gke-logs-prod
gke-logs-prod: ## Tail application logs in canon-prod
	kubectl logs -f -l app -n canon-prod --all-containers --max-log-requests=10

.PHONY: gke-logs-staging
gke-logs-staging: ## Tail application logs in canon-staging
	kubectl logs -f -l app -n canon-staging --all-containers --max-log-requests=10

.PHONY: gke-restart-prod
gke-restart-prod: ## Rollout restart all deployments in canon-prod
	@for deploy in $(GKE_DEPLOYS); do \
		kubectl rollout restart deployment/$$deploy -n canon-prod; \
	done
	@echo "==> canon-prod restarting."

.PHONY: gke-restart-staging
gke-restart-staging: ## Rollout restart all deployments in canon-staging
	@for deploy in $(GKE_DEPLOYS); do \
		kubectl rollout restart deployment/$$deploy -n canon-staging; \
	done
	@echo "==> canon-staging restarting."

.PHONY: gke-monitoring-setup
gke-monitoring-setup: ## Create uptime checks and email alert channel (requires ALERT_EMAIL)
	@if [ -z "$(ALERT_EMAIL)" ]; then \
		echo "ERROR: ALERT_EMAIL is required. Usage: make gke-monitoring-setup ALERT_EMAIL=you@example.com"; \
		exit 1; \
	fi
	@echo "==> Creating email notification channel..."
	gcloud alpha monitoring channels create \
		--display-name="Canon Alerts" \
		--type=email \
		--channel-labels=email_address=$(ALERT_EMAIL)
	@echo "==> Creating prod uptime check..."
	gcloud monitoring uptime create \
		--display-name="Canon Prod" \
		--resource-type=uptime-url \
		--hostname=canon.mopjones.com \
		--path=/health \
		--protocol=HTTPS \
		--period=5m
	@echo "==> Creating staging uptime check..."
	gcloud monitoring uptime create \
		--display-name="Canon Staging" \
		--resource-type=uptime-url \
		--hostname=canon-staging.mopjones.com \
		--path=/health \
		--protocol=HTTPS \
		--period=5m
	@echo "==> Monitoring setup complete."
