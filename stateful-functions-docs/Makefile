# Copyright 2019 Ververica GmbH.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ________________________________________________________ 
#  _________/\\\___________________________________________
#   ________\/\\\___________________________________________
#    ________\/\\\___________________________________________
#     ________\/\\\______/\\\\\________/\\\\\\\\__/\\\\\\\\\\_
#      ___/\\\\\\\\\____/\\\///\\\____/\\\//////__\/\\\//////__
#       __/\\\////\\\___/\\\__\//\\\__/\\\_________\/\\\\\\\\\\_
#        _\/\\\__\/\\\__\//\\\__/\\\__\//\\\________\////////\\\_
#         _\//\\\\\\\/\\__\///\\\\\/____\///\\\\\\\\__/\\\\\\\\\\_
#          __\///////\//_____\/////________\////////__\//////////__
#           ________________________________________________________
# 
# Makefile for Ververica Platform Docs
# 

BUILDDIR     = _build
DOCKER_GROUP = $(or $(shell printenv DOCKER_GROUP), $(GID), 1000)
DOCKER_USER  = $(or $(shell printenv DOCKER_USER), $(UID), 1000)
DOCS_FILES   = $(shell find . -name \*.rst -print)
SOURCEDIR    = docs
SPHINXBUILD  = sphinx-build
SPHINXOPTS   = -W
SPHINXPROJ   = Stateful Functions


# Target called by netlify, does some setup tasks specific to netlify
.PHONY: multiversion
multiversion:
	sphinx-versioning build "$(SOURCEDIR)" "$(BUILDDIR)/html"

# Target for building the docs on netlify
.PHONY: netlify
netlify:
	./netlify-build.sh

# Run sphinx-autobuild to watch and rebuild the docs on changes
.PHONY: autobuild
autobuild:
	sphinx-autobuild \
	  --host 0.0.0.0 \
	  --port 8000 \
	  --ignore "*.sw?" \
	  --ignore "*.sw??" \
	  --ignore "*~" \
	  "$(SOURCEDIR)" \
	  "$(BUILDDIR)/html" \
	  $(SPHINXOPTS) \
	  $(0)

.PHONY: latexpdf
latexpdf:
	sphinx-build \
	  -M latexpdf \
	  "$(SOURCEDIR)" \
	  "$(BUILDDIR)/pdf" \
	  $(SPHINXOPTS) \
	  $(0)

# Build the base docker image for the docker-% target to use
.PHONY: docker
docker:
	docker build -t stateful-functions-docs-builder .

# Run other make targets in docker... obviously don't call another docker target
.PHONY: docker-%
docker-%: docker
	docker \
	  run \
	  --rm \
	  -it \
	  -v $(PWD):/build \
	  --user $(DOCKER_USER):$(DOCKER_GROUP) \
	  -p 8000:8000 \
	  stateful-functions-docs-builder \
	  make -C /build $*

# Run sphinx-autobuild in a docker container, much nicer than running locally
.PHONY: auto
auto: docker-autobuild

.PHONY: html
html:
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Legacy build hosted in a docker container, then deployed to kubernetes
.PHONY: dist
dist: docker-html
	rsync -a _build/html dist/
	make -C dist

# Remove/clean the build directory
.PHONY: clean
clean:
	rm -rf "$(BUILDDIR)"

build-linter: Dockerfile-linter
	docker build -f Dockerfile-linter -t stateful-functions/stateful-functions-docs-linter .
	touch $@

.PHONY: docker-linter
docker-linter: build-linter
	docker \
	  run \
	  --rm \
	  -it \
	  --name docker-linter \
	  -v ${PWD}:/run/docs \
	  -w /run/docs \
	  stateful-functions/stateful-functions-docs-linter:latest \
	  $(LINTER) $(or $(LINTER_TARGET), $(DOCS_FILES))

.PHONY: linter-alex
linter-alex:
	make docker-linter LINTER=alex

.PHONY: linter-mdspell
linter-mdspell:
	make docker-linter LINTER="mdspell --en-us --ignore-numbers --ignore-acronyms --report"

.PHONY: linter-proselint
linter-proselint:
	make docker-linter LINTER=proselint

.PHONY: linter-retext-mapbox-standard
linter-retext-mapbox-standard:
	make docker-linter LINTER=retext-mapbox-standard

.PHONY: linter-write-good
linter-write-good:
	make docker-linter LINTER=write-good

.PHONY: lint
lint:
	make linter-write-good || true
	make linter-retext-mapbox-standard || true
	make linter-alex || true
	make linter-proselint || true

# This recreates the netlify build environment for local testing and troubleshooting.
#  If you're really stumped, try running it with BUILD_ARGS=/bin/bash to get a shell
#  and look around the filesystem to see whats up.
.PHONY: netlify-build-local
netlify-build-local:
	@mkdir -p _build
	@chmod g+w _build
	docker \
	  run \
	  --rm \
	  -it \
	  -v $(PWD):/opt/buildhome/local_repo \
	  -e NETLIFY_REPO_URL=/opt/buildhome/local_repo \
	  --user buildbot:$(DOCKER_GROUP) \
	  netlify/build \
	  build $(or $(BUILD_ARGS), ./netlify-build-local.sh)
