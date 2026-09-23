# mk/release.mk — go-objstore's signed -alpha release path (issue #32).
#
#   make release-alpha VERSION=vX.Y.Z-alpha
#
# Included by the Makefile, and by mk/testdata/release-path-tests.sh, which runs
# this exact wiring against a throwaway remote (make test-release-alpha).
#
# release-alpha runs, in order:
#   1. release-alpha-lockstep  refuses while SUBMODULE_TAGS names a nested module
#   2. changelog-assemble      folds changelog.d/ fragments and the Unreleased
#                              section into the version's CHANGELOG section and
#                              commits it, signed, BEFORE the tag exists
#                              (release-versioning.md §5)
#   3. the template recipe     writes VERSION, commits, creates the annotated
#                              tag and pushes only the release commit and that tag
#
# mk/release-alpha.mk is ../handbook/software/templates/make/release-alpha.mk
# byte-for-byte, and mk/testdata/release-alpha-tests.sh is its own regression
# harness. Update both by re-copying from the handbook, never by editing them
# here; repo-specific behaviour lives in this file.
#
# The version is explicit (release-versioning.md decides patch/minor/major),
# every git write signs through agent-git.sh as agent-go-objstore, the tag is
# annotated, and only the release commit and that one tag are pushed.
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

# The directory holding this file, captured before any further include moves
# MAKEFILE_LIST. `override` so a command-line value cannot redirect the recipe.
override RELEASE_MK_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

include $(RELEASE_MK_DIR)release-alpha.mk

# The template tags the root module only. SUBMODULE_TAGS is empty today; if a
# nested module is ever added to it, release-alpha must refuse rather than
# silently skip the lockstep tag, until the recipe learns to push it.
unexport SUBMODULE_TAGS
.PHONY: release-alpha-lockstep
release-alpha-lockstep:
	$(if $(strip $(value SUBMODULE_TAGS)),@echo 'ERROR: SUBMODULE_TAGS is set but release-alpha (template v2.3.9) tags and pushes the root module only - cut the lockstep tags by hand per release-versioning.md'; exit 1,@:)

# changelog-assemble — mk/changelog-assemble.sh does the work. Its inputs cross
# into the recipe exactly as the template's do (template v2.3.3/v2.3.7/v2.3.8):
# $(value ...) so nothing is expanded as recipe text, `override` so the
# internal CL_* names cannot be set from the command line, and `unexport` so a
# command-line value is not expanded while building the recipe environment.
# The public inputs (VERSION, AGENT_GIT, REPO_NAME, RELEASE_REMOTE) are already
# unexported by the template.
.PHONY: changelog-assemble
unexport CL_VERSION CL_REMOTE CL_REPO CL_AGENT_GIT
changelog-assemble: override export CL_VERSION   := $(value VERSION)
changelog-assemble: override export CL_REMOTE    := $(value RELEASE_REMOTE)
changelog-assemble: override export CL_REPO      := $(value REPO_NAME)
changelog-assemble: override export CL_AGENT_GIT := $(value AGENT_GIT)
changelog-assemble:
	@bash '$(RELEASE_MK_DIR)changelog-assemble.sh'

# Order matters: the lockstep check writes nothing and must pass before the
# assembly step may run. Sibling prerequisites carry no order under make -j, so
# assembly depends on the check directly; a rejected release never assembles or
# commits CHANGELOG changes. The assembly commit must exist before the template
# records VERSION and tags.
changelog-assemble: release-alpha-lockstep
release-alpha: release-alpha-lockstep changelog-assemble

.PHONY: test-release-alpha
## test-release-alpha: Run the release-alpha regression harnesses against a throwaway remote
test-release-alpha:
	@bash '$(RELEASE_MK_DIR)testdata/release-alpha-tests.sh'
	@bash '$(RELEASE_MK_DIR)testdata/release-path-tests.sh'
