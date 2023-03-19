import streamlit as st


def main() -> None:
    st.markdown(
"""
# Censo escolar
Apresenta graficamente informações sobre os microdados e indicadores educacionais do 
Censo Escolar no período de 2016 até 2022. Para executar o projeto localmente, leia o [README](https://github.com/rjribeiro/tcc_censo_escolar#readme).

### Métricas 
- Acesso a serviços básicos
    - Abastecimento de água
    - Abastecimento de energia elétrica
    - Esgoto sanitário
    - Destinação do lixo
    - Acesso a internet
    
- Quantidade de escolas

- Quantidade matrículas

- Indicadores educations
    - Adequação da Formação Docente
    - Percentual de Docentes com Curso Superior
    - Índice de Esforço Docente
    - Média de Alunos por Turma
    - Média de Horas-aula Diária
    - Taxas de Distorção Idade-série
    - Taxa de Aprovação
    - Taxa de Reprovação
    - Taxa de Abandono

### Dimensões
- Localização geográfica
    - País
    - Região Geográfica
    - Unidade da Federação
    - Mesorregião
    - Microrregião
    - Município
    
- Localidade
    - Urbana
    - Rural
    
- Dependência Administrativa
    - Municipal
    - Estadual
    - Federal
    - Privada

- Subtipo de servico básico
"""
)


if __name__ == "__main__":
    main()
