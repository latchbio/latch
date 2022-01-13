local_install:
  python -m pip install -e .

build:
  python setup.py sdist bdist_wheel

publish:
  twine upload dist/*
