# How to run:

## a) Setup using venv:

(Python 3.9 is preferable, but 3.8 should work, probably also 3.7)

1.     python -m venv venv
2.     source venv/bin/activate
3.     pip install -r requirements.txt

## b) Setup using conda:

1.     conda env update
2.     conda activate rtree

## Run CLI interface:

    python main.py

## Run tests:

    pytest

### Run code checkers

    pylint rtree
    mypy -p rtree