.DEFAULT_GOAL := default

run:
	docker-compose up -d
	# https://github.com/apache/airflow/issues/14266
	docker exec -it udacity-data-engineering-capstone_airflow-webserver_1 bash -c " \
	pip uninstall --yes azure-storage && \
	pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0 \
	"

stop:
	docker-compose down

logs:
	docker-compose logs -f

cleanup: stop
	docker-compose rm

prep-venv:
	rm -rf venv
	python3 -m pip install virtualenv
	python3 -m venv venv
	venv/bin/python3 -m pip install --upgrade pip
	venv/bin/python3 -m pip install -r requirements.txt

prep-venv-dev: prep-venv
	venv/bin/python3 -m pip install -r requirements-dev.txt

prep-source:
	venv/bin/python3 data_prep/download_us_demographic_ds.py

default: run logs
