venv:
	python3 -mvenv $@
	$@/bin/pip install -e .[test]
