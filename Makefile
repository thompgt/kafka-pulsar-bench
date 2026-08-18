# kafka-pulsar-bench — developer entry points.
#
# Everything that touches infrastructure goes through here so that the
# teardown discipline required by invariant 4 is not something anyone has to
# remember.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE_DIR   := infra/compose
VERSIONS_FILE := $(COMPOSE_DIR)/versions.env
LOCAL_ENV     := $(COMPOSE_DIR)/local.env

# Local overrides are optional; ports and resource limits are the usual reason.
ENV_FILES := --env-file $(VERSIONS_FILE)
ifneq (,$(wildcard $(LOCAL_ENV)))
ENV_FILES += --env-file $(LOCAL_ENV)
endif

PROFILE ?= kafka
CORE_FILE   := -f $(COMPOSE_DIR)/docker-compose.core.yml
BROKER_FILE := $(if $(filter none,$(PROFILE)),,-f $(COMPOSE_DIR)/docker-compose.$(PROFILE).yml)
COMPOSE     := docker compose $(ENV_FILES) $(CORE_FILE) $(BROKER_FILE)

VALID_PROFILES := kafka pulsar none

.PHONY: help
help: ## Show this help
	@echo "kafka-pulsar-bench"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  PROFILE=kafka|pulsar|none   (default: kafka)"

.PHONY: check-profile
check-profile:
	@if ! echo "$(VALID_PROFILES)" | grep -qw "$(PROFILE)"; then \
		echo "ERROR: PROFILE must be one of: $(VALID_PROFILES) (got '$(PROFILE)')" >&2; exit 1; \
	fi

.PHONY: preflight
preflight: ## Check Docker memory, disk, and port availability
	@bash scripts/preflight.sh

.PHONY: up
up: check-profile preflight ## Bring up core + one broker, blocking until healthy
	@bash scripts/up.sh "$(PROFILE)"

.PHONY: down
down: check-profile ## Stop containers, keep the warehouse volume
	$(COMPOSE) down --remove-orphans

.PHONY: nuke
nuke: ## Stop everything and delete ALL volumes, warehouse included
	@echo "This deletes the Iceberg warehouse as well as broker state."
	@read -p "Type 'nuke' to confirm: " ans; [ "$$ans" = "nuke" ] || { echo "aborted"; exit 1; }
	docker compose $(ENV_FILES) $(CORE_FILE) \
		-f $(COMPOSE_DIR)/docker-compose.kafka.yml \
		-f $(COMPOSE_DIR)/docker-compose.pulsar.yml \
		down --volumes --remove-orphans

.PHONY: reset-broker
reset-broker: check-profile ## Destroy and rebuild ONLY the broker (invariant 4)
	@bash scripts/reset-broker.sh "$(PROFILE)"

.PHONY: ps
ps: check-profile ## Show container status
	$(COMPOSE) ps

.PHONY: logs
logs: check-profile ## Tail logs
	$(COMPOSE) logs -f --tail=100

.PHONY: digests
digests: ## Re-resolve image digests into images.lock
	@bash scripts/resolve-digests.sh

.PHONY: digests-check
digests-check: ## Fail if images.lock is stale
	@bash scripts/resolve-digests.sh --check
