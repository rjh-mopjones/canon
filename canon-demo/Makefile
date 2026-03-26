# Canon Demo — Kubernetes deployment via minikube / GKE
#
# Minikube:
#   make k8s-up      — full stack: start minikube, build images, deploy
#   make k8s-down    — tear down the canon namespace
#   make k8s-build   — cross-compile services locally, build slim Docker images
#   make k8s-deploy  — apply Kustomize manifests
#   make k8s-status  — show pod/job/service status
#   make k8s-logs    — tail logs from all canon pods
#   make k8s-test-e2e — run Playwright e2e smoke tests against running cluster
#   make k8s-tunnel  — expose LoadBalancer services via minikube tunnel
#
# GKE:
#   make gke-build-push      — cross-compile x86_64, push images to Artifact Registry
#   make gke-deploy           — deploy prod + staging + infra policies
#   make gke-deploy-prod      — deploy prod overlay
#   make gke-deploy-staging   — deploy staging overlay
#   make gke-status           — pods, services, ingress across all namespaces
#   make gke-logs-prod        — tail prod application logs
#   make gke-logs-staging     — tail staging application logs
#   make gke-restart-prod     — rollout restart prod
#   make gke-restart-staging  — rollout restart staging
#   make gke-monitoring-setup — create uptime checks + email alerts (ALERT_EMAIL= required)

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
		docker build -t canon-demo/$(svc) \
			--build-arg BINARY_PATH=target/$(CROSS_TARGET)/release/$(svc) \
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

# ── GKE Targets ────────────────────────────────────────────────────────────────

GKE_REGISTRY := europe-west2-docker.pkg.dev/canon-demo-prod/canon
GKE_CROSS_TARGET := x86_64-unknown-linux-musl
GKE_SERVICES := fleet-service cargo-service navigation-service supply-service station-service gateway
GKE_DEPLOYMENTS := fleet-service cargo-service navigation-service supply-service station-service gateway frontend
GKE_TAG ?= latest

.PHONY: gke-build-push
gke-build-push: ## Cross-compile x86_64 and push all images to Artifact Registry
	@echo "==> Cross-compiling services for $(GKE_CROSS_TARGET)..."
	cargo build --release --target $(GKE_CROSS_TARGET) \
		$(foreach svc,$(GKE_SERVICES),-p $(svc))
	@echo "==> Configuring Docker for Artifact Registry..."
	gcloud auth configure-docker europe-west2-docker.pkg.dev --quiet
	@echo "==> Building and pushing service images..."
	@$(foreach svc,$(GKE_SERVICES), \
		echo "    building + pushing $(svc)..." && \
		docker build --no-cache -t $(GKE_REGISTRY)/$(svc):$(GKE_TAG) \
			--build-arg BINARY_PATH=target/$(GKE_CROSS_TARGET)/release/$(svc) \
			-f $(svc)/Dockerfile ../. && \
		docker push $(GKE_REGISTRY)/$(svc):$(GKE_TAG) && \
	) true
	@echo "==> Building and pushing frontend image..."
	docker build --no-cache -t $(GKE_REGISTRY)/frontend:$(GKE_TAG) -f frontend/Dockerfile ../.
	docker push $(GKE_REGISTRY)/frontend:$(GKE_TAG)
	@echo "==> Building and pushing init-schema image..."
	docker build --no-cache -t $(GKE_REGISTRY)/init-schema:$(GKE_TAG) init-schema/
	docker push $(GKE_REGISTRY)/init-schema:$(GKE_TAG)
	@echo "==> All images built and pushed to $(GKE_REGISTRY)."

.PHONY: gke-deploy-prod
gke-deploy-prod: ## Apply prod overlay and wait for rollouts in canon-prod
	@echo "==> Applying GKE prod overlay..."
	kubectl apply -k k8s/overlays/gke/prod/
	@echo "==> Waiting for rollouts in canon-prod..."
	@$(foreach deploy,$(GKE_DEPLOYMENTS), \
		echo "    waiting for $(deploy)..." && \
		kubectl rollout status deployment/$(deploy) -n canon-prod --timeout=300s && \
	) true
	@echo "==> All prod deployments rolled out."

.PHONY: gke-deploy-staging
gke-deploy-staging: ## Apply staging overlay and wait for rollouts in canon-staging
	@echo "==> Applying GKE staging overlay..."
	kubectl apply -k k8s/overlays/gke/staging/
	@echo "==> Waiting for rollouts in canon-staging..."
	@$(foreach deploy,$(GKE_DEPLOYMENTS), \
		echo "    waiting for $(deploy)..." && \
		kubectl rollout status deployment/$(deploy) -n canon-staging --timeout=300s && \
	) true
	@echo "==> All staging deployments rolled out."

.PHONY: gke-deploy
gke-deploy: gke-deploy-prod gke-deploy-staging ## Deploy both prod and staging + infra policies
	@echo "==> Applying infra network policies..."
	kubectl apply -k k8s/overlays/gke/infra-policies/
	@echo "==> GKE deploy complete (prod + staging + infra policies)."

.PHONY: gke-status
gke-status: ## Show pods, services, and ingress across all GKE namespaces
	@for ns in canon-prod canon-staging canon-infra; do \
		echo ""; \
		echo "=== Namespace: $$ns ==="; \
		echo "--- Pods ---"; \
		kubectl get pods -n $$ns -o wide 2>/dev/null || echo "  (namespace not found)"; \
		echo "--- Services ---"; \
		kubectl get svc -n $$ns 2>/dev/null || echo "  (namespace not found)"; \
		echo "--- Ingress ---"; \
		kubectl get ingress -n $$ns 2>/dev/null || echo "  (none)"; \
	done

.PHONY: gke-logs-prod
gke-logs-prod: ## Tail application logs in canon-prod
	kubectl logs -n canon-prod -l app.kubernetes.io/part-of=canon-demo --all-containers --follow --max-log-requests=20

.PHONY: gke-logs-staging
gke-logs-staging: ## Tail application logs in canon-staging
	kubectl logs -n canon-staging -l app.kubernetes.io/part-of=canon-demo --all-containers --follow --max-log-requests=20

.PHONY: gke-restart-prod
gke-restart-prod: ## Rollout restart all app deployments in canon-prod
	@echo "==> Restarting deployments in canon-prod..."
	@$(foreach deploy,$(GKE_DEPLOYMENTS), \
		kubectl rollout restart deployment/$(deploy) -n canon-prod && \
	) true
	@echo "==> Waiting for rollouts..."
	@$(foreach deploy,$(GKE_DEPLOYMENTS), \
		kubectl rollout status deployment/$(deploy) -n canon-prod --timeout=300s && \
	) true
	@echo "==> All prod deployments restarted."

.PHONY: gke-restart-staging
gke-restart-staging: ## Rollout restart all app deployments in canon-staging
	@echo "==> Restarting deployments in canon-staging..."
	@$(foreach deploy,$(GKE_DEPLOYMENTS), \
		kubectl rollout restart deployment/$(deploy) -n canon-staging && \
	) true
	@echo "==> Waiting for rollouts..."
	@$(foreach deploy,$(GKE_DEPLOYMENTS), \
		kubectl rollout status deployment/$(deploy) -n canon-staging --timeout=300s && \
	) true
	@echo "==> All staging deployments restarted."

.PHONY: gke-monitoring-setup
gke-monitoring-setup: ## Create uptime checks and email alerts (ALERT_EMAIL= required)
	@if [ -z "$(ALERT_EMAIL)" ]; then \
		echo "Error: ALERT_EMAIL is required. Usage: make gke-monitoring-setup ALERT_EMAIL=you@example.com"; \
		exit 1; \
	fi
	@echo "==> Creating notification channel for $(ALERT_EMAIL)..."
	$(eval CHANNEL_ID := $(shell gcloud alpha monitoring channels create \
		--display-name="Canon Alert Email" \
		--type=email \
		--channel-labels=email_address=$(ALERT_EMAIL) \
		--format='value(name)' 2>/dev/null))
	@echo "    channel: $(CHANNEL_ID)"
	@echo "==> Creating uptime check for canon.mopjones.com (prod)..."
	gcloud monitoring uptime create canon-prod-health \
		--display-name="Canon Prod Health" \
		--resource-type=uptime-url \
		--hostname=canon.mopjones.com \
		--path=/health \
		--protocol=https \
		--period=5 \
		--timeout=10s \
		--checker-type=STATIC_IP_CHECKERS
	@echo "==> Creating uptime check for canon-staging.mopjones.com (staging)..."
	gcloud monitoring uptime create canon-staging-health \
		--display-name="Canon Staging Health" \
		--resource-type=uptime-url \
		--hostname=canon-staging.mopjones.com \
		--path=/health \
		--protocol=https \
		--period=5 \
		--timeout=10s \
		--checker-type=STATIC_IP_CHECKERS
	@echo "==> Creating alert policies..."
	gcloud alpha monitoring policies create \
		--display-name="Canon Prod Uptime Alert" \
		--condition-display-name="Prod health check failing" \
		--condition-filter='resource.type = "uptime_url" AND metric.type = "monitoring.googleapis.com/uptime_check/check_passed" AND metric.labels.check_id = "canon-prod-health"' \
		--condition-threshold-value=1 \
		--condition-threshold-comparison=COMPARISON_LT \
		--condition-threshold-duration=300s \
		--condition-threshold-aggregation='{"alignmentPeriod":"300s","perSeriesAligner":"ALIGN_NEXT_OLDER","crossSeriesReducer":"REDUCE_COUNT_FALSE","groupByFields":["resource.label.host"]}' \
		--notification-channels=$(CHANNEL_ID) \
		--combiner=OR \
		--documentation='Canon prod (canon.mopjones.com) health check is failing.'
	gcloud alpha monitoring policies create \
		--display-name="Canon Staging Uptime Alert" \
		--condition-display-name="Staging health check failing" \
		--condition-filter='resource.type = "uptime_url" AND metric.type = "monitoring.googleapis.com/uptime_check/check_passed" AND metric.labels.check_id = "canon-staging-health"' \
		--condition-threshold-value=1 \
		--condition-threshold-comparison=COMPARISON_LT \
		--condition-threshold-duration=300s \
		--condition-threshold-aggregation='{"alignmentPeriod":"300s","perSeriesAligner":"ALIGN_NEXT_OLDER","crossSeriesReducer":"REDUCE_COUNT_FALSE","groupByFields":["resource.label.host"]}' \
		--notification-channels=$(CHANNEL_ID) \
		--combiner=OR \
		--documentation='Canon staging (canon-staging.mopjones.com) health check is failing.'
	@echo "==> Monitoring setup complete. Alerts will be sent to $(ALERT_EMAIL)."
