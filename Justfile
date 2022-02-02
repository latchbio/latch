local_install:
  python -m pip install -e .

build:
  rm -rf __pycache__ dist build latch.egg-info
  python setup.py sdist bdist_wheel

publish:
  twine upload dist/*
