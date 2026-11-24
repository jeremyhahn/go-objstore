# Signed -alpha release path (make release-alpha VERSION=vX.Y.Z-alpha): changelog-assemble, then the template recipe tags the root, backup and api/sdks/go in one atomic push.
# mk/template/ is a byte-for-byte copy of the handbook's make templates; repo-specific behaviour lives here.
ifeq ($(origin AGENT_GIT),undefined)
AGENT_GIT := $(shell command -v agent-git.sh 2>/dev/null)
endif
REPO_NAME ?= go-objstore
RELEASE_REMOTE ?= origin
ifeq ($(origin LOCKSTEP_MODULES),undefined)
LOCKSTEP_MODULES := backup api/sdks/go
endif
# The release commit drops the in-tree replaces of backup and api/sdks/go; the commit after the tags restores them.
ifeq ($(origin RELEASE_REPLACES),undefined)
RELEASE_REPLACES := backup api/sdks/go
endif
ifeq ($(origin RELEASE_SYNC_TARGETS),undefined)
RELEASE_SYNC_TARGETS := version-sync version-check
endif
ifeq ($(origin RELEASE_CONSUMER_MODULES),undefined)
RELEASE_CONSUMER_MODULES := . backup api/sdks/go
endif

# The directory holding this file, captured before any further include moves
# MAKEFILE_LIST. `override` so a command-line value cannot redirect the recipe.
override RELEASE_MK_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

include $(RELEASE_MK_DIR)template/release-alpha.mk

# Inputs cross into the recipe via $(value), override and unexport, as in the template.
.PHONY: changelog-assemble
unexport CL_VERSION CL_REMOTE CL_REPO CL_AGENT_GIT CL_MODULES
changelog-assemble: override export CL_VERSION   := $(value VERSION)
changelog-assemble: override export CL_REMOTE    := $(value RELEASE_REMOTE)
changelog-assemble: override export CL_REPO      := $(value REPO_NAME)
changelog-assemble: override export CL_AGENT_GIT := $(value AGENT_GIT)
changelog-assemble: override export CL_MODULES   := $(value LOCKSTEP_MODULES)
changelog-assemble:
	@bash '$(RELEASE_MK_DIR)changelog-assemble.sh'

# The assembly commit precedes the template's VERSION write and tags, with the same tag checks.
release-alpha: changelog-assemble

.PHONY: test-release-alpha
## test-release-alpha: Run the release-alpha regression harnesses against a throwaway remote
test-release-alpha:
	@bash '$(RELEASE_MK_DIR)template/testdata/release-alpha-tests.sh'
	@bash '$(RELEASE_MK_DIR)testdata/release-path-tests.sh'
