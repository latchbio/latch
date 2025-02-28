# Setup

install:
  uv sync --group dev --group docs --no-cache --frozen

# Packaging

build:
  rm -rf dist
  uv build

publish:
  uv publish --token $(<credentials/pypi_token)
  rm -rf dist

# Testing

test:
  export TEST_TOKEN=$(cat ~/.latch/token)
  pytest -s

# Docs

build-api-docs:
  rm docs/source/api/*
  sphinx-apidoc \
    --force \
    -o docs/source/api/ . \
    'latch_cli/services/init/*/**' \
    'latch_cli/snakemake' \
    'tests/*'

build-docs:
  make --directory docs html

git_hash := `git rev-parse --short=4 HEAD`
git_branch := `inp=$(git rev-parse --abbrev-ref HEAD); echo "${inp//\//--}"`

docker_image_name := "sdk-docs"
docker_registry := "812206152185.dkr.ecr.us-west-2.amazonaws.com"
docker_image_version := docker_image_name + "-" + git_hash + "-" + git_branch
docker_image_full := docker_registry + "/" + docker_image_name + ":" + docker_image_version

@docker-login:
  aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin {{docker_registry}}

@docker-build: build
  docker build -t {{docker_image_full}} . -f Dockerfile.docs

@docker-push:
  docker push {{docker_image_full}}

@dbnp: build-docs docker-build docker-push
