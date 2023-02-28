setup_etl:
	docker build -t etl etl

run_etl:
	docker run -v ${PWD}/data:/censo/data -v ${PWD}/etl:/censo etl python extract/extract.py
	docker run -v ${PWD}/data:/censo/data -v ${PWD}/etl:/censo etl python transform/transform.py

setup_app:
	docker build -t app app

run_app:
	docker run  -v ${PWD}/data/transformed.parquet:/app/data/transformed.parquet  -v ${PWD}/app:/app/app -p 8501:8501 app


