# tcc_censo_escolar

ETL e visualização do Censo escolar. [Link para o data app no Streamlit Community Cloud](https://censo-escolar.streamlit.app/)

## Setup

- Instale [Python 3.10](https://www.python.org/downloads/)
- Crie um ambiente virtual para o seu Python. Aqui o código utiliando virtualenv:
```python3.10 -m venv venv && source venv/bin/activate```
- Instale as dependências:
```pip install -r requirements.txt```

## Extração e Transformação

Os scripts devem ser executados a partir do caminho raiz do repositório usando
o ambiente virtual criado acima.

### microdados
- [etl/microdados/extract.py](etl/microdados/extract.py)
- [etl/microdados/transform.py](etl/microdados/transform.py)

### indicadores
- [etl/indicadores/extract.py](etl/indicadores/extract.py)
- [etl/indicadores/transform.py](etl/indicadores/transform.py)


## App

Usando o ambiente virtual criado acima e estando na raiz do repositório, execute
```streamlit run app/home.py```



