setup_etl:
	docker build -t etl etl

run_etl:
	docker run -v ${PWD}/data:/censo/data etl python extract/extract.py
	docker run -v ${PWD}/data:/censo/data etl python transform/transform.py

setup_app:
	docker build -t app .

run_app:
	docker run  -v ${PWD}/data/transformed.parquet:/app/data/transformed.parquet  -v ${PWD}:/app -p 8501:8501 app


