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
#   2. the template recipe     writes VERSION, re-pins every requirement the
#                              nested modules have on the root and SDK modules to
#                              the version, drops the in-repo replaces of the
#                              RELEASE_REPLACES modules, runs RELEASE_SYNC_TARGETS,
#                              and commits that as the release commit, which
#                              carries the three annotated tags. It then commits
#                              the replaces back (requirements still at the new
#                              version) so main keeps building against the
#                              in-tree modules, pushes both commits and exactly
#                              the three tags in one atomic push, and checks each
#                              RELEASE_CONSUMER_MODULES module as an outside
#                              consumer at the new tag.
#
# mk/template/ is ../handbook/software/templates/make/ byte for byte
# (release-common.mk 1.2.0, release-alpha.mk, release.mk, release-candidate.mk
# and the regression harness testdata/release-alpha-tests.sh). Update it by
# re-copying from the handbook; repo-specific behaviour lives in this file.
#
# The version is explicit (release-versioning.md decides patch/minor/major),
# every git write signs through agent-git.sh as this project's agent, the tags are
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
# The template's opt-in inputs (release-common.mk 1.2.0 header). backup and
# api/sdks/go replace the root and SDK modules with their in-tree directories;
# the release commit drops those replaces so `go install <module>/cmd/<x>@<tag>`
# works, and the restore commit after the tags puts them back.
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

.PHONY: test-release-alpha
## test-release-alpha: Run the release-alpha regression harnesses against a throwaway remote
test-release-alpha:
	@bash '$(RELEASE_MK_DIR)template/testdata/release-alpha-tests.sh'
	@bash '$(RELEASE_MK_DIR)testdata/release-path-tests.sh'
