# mk/release.mk — go-objstore's signed -alpha release path.
#
#   make release-alpha VERSION=vX.Y.Z-alpha
#
# Included by the Makefile, and by mk/testdata/release-path-tests.sh, which runs
# this exact wiring against a throwaway remote (make test-release-alpha).
#
# One release cuts three tags in lockstep (release-versioning.md §6,
# dependency-resolution.md §5): vX.Y.Z-alpha for the root module and
# backup/vX.Y.Z-alpha and api/sdks/go/vX.Y.Z-alpha for the nested modules named
# in LOCKSTEP_MODULES. release-alpha runs, in order:
#   1. changelog-assemble      refuses, writing nothing, if any of the three tags
#                              exists locally or on the remote; then folds
#                              changelog.d/ fragments and the Unreleased section
#                              into the version's CHANGELOG section and commits
#                              it, signed, BEFORE any tag exists
#                              (release-versioning.md §5)
#   2. the template recipe     writes VERSION, runs version-sync and version-check,
#                              re-pins every requirement the nested modules have
#                              on the root and SDK modules to the version, drops
#                              their in-repo replace directives, and commits that
#                              as the release commit, which carries the three
#                              annotated tags. It then commits the replaces back
#                              (requirements still at the new version) so main
#                              keeps building against the in-tree modules, pushes
#                              both commits and exactly the three tags in one
#                              atomic push, and runs release-consumer-check.
#
# mk/template/ is ../handbook/software/templates/make/ (release-alpha.mk,
# release-common.mk, release.mk, release-candidate.mk and the regression harness
# testdata/release-alpha-tests.sh) plus the local additions listed in the header
# of mk/template/release-common.mk. Update it by re-copying from the handbook and
# re-applying those additions; other repo-specific behaviour lives in this file.
#
# The version is explicit (release-versioning.md decides patch/minor/major),
# every git write signs through agent-git.sh as agent-go-objstore, the tags are
# annotated, and only the release commits and those tags are pushed.
# AGENT_GIT is resolved from PATH — no baked path (template v2.3.6 (a)); an
# unresolvable wrapper leaves it empty and the recipe fails before anything is
# written. It is a SIMPLE (:=) assignment on purpose: the template reads it
# through $(value AGENT_GIT), which on a recursive `?=` variable would return
# the literal text "$(shell ...)" rather than the resolved path.
ifeq ($(origin AGENT_GIT),undefined)
AGENT_GIT := $(shell command -v agent-git.sh 2>/dev/null)
endif
REPO_NAME ?= go-objstore
RELEASE_REMOTE ?= origin
ifeq ($(origin LOCKSTEP_MODULES),undefined)
LOCKSTEP_MODULES := backup api/sdks/go
endif

# The directory holding this file, captured before any further include moves
# MAKEFILE_LIST. `override` so a command-line value cannot redirect the recipe.
override RELEASE_MK_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

include $(RELEASE_MK_DIR)template/release-alpha.mk

# changelog-assemble — mk/changelog-assemble.sh does the work. Its inputs cross
# into the recipe exactly as the template's do (template v2.3.3/v2.3.7/v2.3.8):
# $(value ...) so nothing is expanded as recipe text, `override` so the
# internal CL_* names cannot be set from the command line, and `unexport` so a
# command-line value is not expanded while building the recipe environment.
# The public inputs (VERSION, AGENT_GIT, REPO_NAME, RELEASE_REMOTE,
# LOCKSTEP_MODULES) are already unexported by the template.
.PHONY: changelog-assemble
unexport CL_VERSION CL_REMOTE CL_REPO CL_AGENT_GIT CL_MODULES
changelog-assemble: override export CL_VERSION   := $(value VERSION)
changelog-assemble: override export CL_REMOTE    := $(value RELEASE_REMOTE)
changelog-assemble: override export CL_REPO      := $(value REPO_NAME)
changelog-assemble: override export CL_AGENT_GIT := $(value AGENT_GIT)
changelog-assemble: override export CL_MODULES   := $(value LOCKSTEP_MODULES)
changelog-assemble:
	@bash '$(RELEASE_MK_DIR)changelog-assemble.sh'

# The assembly commit must exist before the template records VERSION and tags.
# Its pre-flight checks the same tags the template's guard does, so a release the
# template would refuse never commits CHANGELOG changes.
release-alpha: changelog-assemble

# consumer-check — post-tag consumer verification of one module at one tag:
#   make consumer-check MODULE=github.com/jeremyhahn/go-objstore/backup VERSION=v0.9.0-alpha
# Inputs cross into the recipe as changelog-assemble's do.
.PHONY: consumer-check
unexport CC_MODULE CC_VERSION MODULE
consumer-check: override export CC_MODULE  := $(value MODULE)
consumer-check: override export CC_VERSION := $(value VERSION)
consumer-check:
	@bash '$(RELEASE_MK_DIR)consumer-check.sh'

# release-consumer-check — consumer-check once per released module: the root and
# every LOCKSTEP_MODULES entry, at the version in VERSION. release-alpha runs it
# after the push; run it by hand to re-verify a published release.
.PHONY: release-consumer-check
unexport RCC_MODULES
release-consumer-check: override export RCC_MODULES := $(value LOCKSTEP_MODULES)
release-consumer-check:
	@set -e; v="v$$(cat VERSION)"; root="$$(GOWORK=off GOFLAGS= go list -m)"; \
	for m in "$$root" $$(for d in $$RCC_MODULES; do printf '%s/%s ' "$$root" "$$d"; done); do \
	  $(MAKE) --no-print-directory consumer-check MODULE="$$m" VERSION="$$v"; \
	done

.PHONY: test-release-alpha
## test-release-alpha: Run the release-alpha regression harnesses against a throwaway remote
test-release-alpha:
	@bash '$(RELEASE_MK_DIR)template/testdata/release-alpha-tests.sh'
	@bash '$(RELEASE_MK_DIR)testdata/release-path-tests.sh'
