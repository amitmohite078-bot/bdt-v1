# SmartProtein Lakehouse & Recommender - Makefile
# Targets: alltest, test, dq-test, lint, clean

.PHONY: alltest test dq-test lint clean help

help:
	@echo "SmartProtein Test Commands:"
	@echo "  make alltest     - Run complete test suite (pytest + DQ validation)"
	@echo "  make test        - Run unit and integration tests with pytest"
	@echo "  make dq-test     - Run Great Expectations DQ gate checks"
	@echo "  make lint        - Syntax compilation and static code check"
	@echo "  make clean       - Remove cached Python pyc files and temp dumps"

alltest: test dq-test
	@echo "=========================================================="
	@echo " [SUCCESS] All SmartProtein Unit, Integration & DQ Tests Passed!"
	@echo "=========================================================="

test:
	pytest tests/ -v --tb=short

dq-test:
	python smartprotein/tests/run_dq_validation_suite.py

lint:
	python -m py_compile app.py smartprotein/dashboard/app.py index.html

clean:
	python -c "import shutil, glob; [shutil.rmtree(p, ignore_errors=True) for p in glob.glob('**/__pycache__', recursive=True)]"
