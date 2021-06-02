import apache_beam as beam

from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from typing import List, Dict, Tuple, Any

'''
https://beam.apache.org/documentation/patterns/pipeline-options/
https://beam.apache.org/documentation/transforms/python/elementwise/map/

'''


pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

# uma Pcolection ou pipeline, PCollectionvocê cria serve como entrada para a primeira operação em seu pipeline.
colunas_dengue = ['id',
                  'data_iniSE',
                  'casos',
                  'ibge_code',
                  'cidade',
                  'uf',
                  'cep',
                  'latitude',
                  'longitude']


def texto_para_lista(elemento: str, delimitador: str = '|') -> List:
    """Realiza um split da linha do dataset a partir
        delimitador especificado.

    Args:
        elemento (str): elemento a ser sliptado, linha do dataset
        delimitador (str, optional): separador. Padrão '|'.

    Returns:
        list: elementos splitados em lista
    """
    return elemento.split(delimitador)


def lista_para_dicionario(elemento: List, colunas: List) -> Dict:
    """Transformando as listas em dicionários.

    Args:
        elemento (List): elementos do dataset
        colunas (List): colunas do dataset

    Returns:
        Dict: dicionário com os elementos e colunas do dataset
    """
    return dict(zip(colunas, elemento))


def trata_datas(elemento: Dict) -> Dict:
    """Cria o campo ano-mes no dicionário.

    Args:
        elemento (Dict): elemento do dataset

    Returns:
        Dict: dicionário com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])

    return elemento


def chave_uf(elemento: Dict) -> Tuple[List, Dict]:
    """Criar uma tupla com o estado(UF) e o elemento (UF, dicionário)

    Args:
        elemento (Dict): elementos do dataset

    Returns:
        Tuple: tupla com o uf e dicionário
    """
    chave = elemento['uf']
    return (chave, elemento)


dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('datasets/casos_dengue.txt', skip_header_lines=1)

    | "De texto para lista" >> beam.Map(texto_para_lista)
    # | "De texto para lista" >> beam.Map(lambda x: x.split('|'))

    # primeiro a chamada da função e depois os parametros dela
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)

    | "Cria campo ano_mes" >> beam.Map(trata_datas)

    | "Criar chave pelo estado" >> beam.Map(chave_uf)

    | "Agrupar pelo estado" >> beam.GroupByKey()

    | "Resultados " >> beam.Map(print)
)

pipeline.run()
