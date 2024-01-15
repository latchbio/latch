local_install:
  python -m pip install -e .

_clear:
  rm -rf __pycache__ dist build latch.egg-info

_build:
  python3 setup.py sdist bdist_wheel

_upload:
  twine upload dist/* -u __token__ -p $(<credentials/pypi_token)

build: _clear _build

publish: _clear _build _upload _clear

requirements:
  pip-compile requirements.in

doc-requirements:
  pip-compile doc-requirements.in

dev-requirements:
  pip-compile dev-requirements.in

build-api-docs:
  rm docs/source/api/*
  sphinx-apidoc \
    --force \
    -o docs/source/api/ . \
    'latch_cli/services/init/*/**' \
    'latch_cli/snakemake' \
    'setup.py' \
    'tests/*'

build-docs:
  make --directory docs html

test:
  export TEST_TOKEN=$(cat ~/.latch/token) &&\
    pytest -s tests

#
# Docs build.

git_hash := `git rev-parse --short=4 HEAD`
git_branch := `inp=$(git rev-parse --abbrev-ref HEAD); echo "${inp//\//--}"`

docker_image_name := "sdk-docs"
docker_registry := "812206152185.dkr.ecr.us-west-2.amazonaws.com"
docker_image_version := docker_image_name + "-" + git_hash + "-" + git_branch
docker_image_full := docker_registry + "/" + docker_image_name + ":" + docker_image_version

@docker-login:
  aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin {{docker_registry}}

@docker-build: build
  docker build -t {{docker_image_full}} . -f Dockerfile-docs

@docker-push:
  docker push {{docker_image_full}}

@dbnp: build-docs docker-build docker-push
