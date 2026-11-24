# Compose project name per CI job and SDK checkout, so concurrent runs on one daemon never collide and `make clean` recomputes it.
# Include from an SDK Makefile:   include ../compose-project.mk

# Needs sha256sum or shasum; with neither the make fails rather than yield a shared name.
ifndef COMPOSE_PROJECT_NAME
compose_project_id := $(GITHUB_RUN_ID)/$(GITHUB_RUN_ATTEMPT)/$(GITHUB_JOB)/$(CURDIR)
compose_project_sq := '
compose_project_quoted := $(subst $(compose_project_sq),'\$(compose_project_sq)',$(compose_project_id))
compose_project_hash := $(shell { printf '%s' '$(compose_project_quoted)' | { sha256sum 2>/dev/null || shasum -a 256 2>/dev/null; }; } | cut -c1-12)
ifneq ($(words $(compose_project_hash)),1)
$(error compose-project.mk: cannot hash the checkout path: install sha256sum or shasum)
endif
COMPOSE_PROJECT_NAME := objstore-sdk-$(lastword $(subst /, ,$(CURDIR)))-$(compose_project_hash)
endif
export COMPOSE_PROJECT_NAME
