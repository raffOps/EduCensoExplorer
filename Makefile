setup_etl:
	docker build -t censo_etl etl

extract_and_transform:
	docker run -v ${PWD}/data/raw:/censo/data/raw censo_etl python extract/extract.py
	docker run -v ${PWD}/data:/censo/data censo python transform/transform.py

setup_app:
	docker build -t app app

run_app:
	docker run  -v ${PWD}/data/transformed.parquet:/app/data/transformed.parquet  -v ${PWD}/app:/app -p 8501:8501 app


