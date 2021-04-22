# How to run:

## Setup:

(Python 3.9 is preferable, but 3.8 should work, probably also 3.7)

1.      python -m venv venv
2.      source venv/bin/activate
3.      pip install -r requirements.txt

## Run CLI interface:

    python main.py

## Run tests:

    pytest

### Run code checkers

    pylint rtree
    mypy -p rtree