setup:
	docker build -t censo .

extract_censo:
	docker run -v ${PWD}/data/raw:/censo/data/raw censo python extract/extract.py

transform_censo:
	docker run -v ${PWD}/data:/censo/data censo python transform/transform.py
