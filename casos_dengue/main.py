from doctest import REPORT_ONLY_FIRST_FAILURE
from stat import UF_APPEND
from sys import argv
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [ 
                "id",
                "data_iniSE",
                "casos",
                "ibge_code",
                "cidade",
                "uf",
                "cep",
                "latitude",
                "longitude"] 

def lista_para_dicionario(elemento, colunas):
    """
    recebe 2 listas
    retorna 1 dicionario
    """
    return dict(zip(colunas, elemento))


def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um demilitador  
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def trata_data(elemento):
    """
    Recebe um dicionario e cria um novo campo ANO-MES
    Retorna o mesmo dicionario com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Receber um dicionario 
    Retornar uma tupla com estado(UF) e o elemento (UF, dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    recebe uma tupla ('RS', [{}, {}])
    retorna uma tupla ('RS-2014-12', 8.0)
    """
    uf, registros = elemento 
    for registros in registros:
        if bool(re.search(r'\d', registros['casos'])):
            yield (f"{uf}-{registros['ano_mes']}", float(registros['casos']))
        else:
            yield (f"{uf}-{registros['ano_mes']}", 0.0)
dengue = (
    pipeline
    | "Leitura do dataset" >> 
        ReadFromText('db2.txt', skip_header_lines = 1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_data)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
 )

pipeline.run()