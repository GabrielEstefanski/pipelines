from sys import argv
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


def lista_para_dicionario(elemento, colunas):
    """
    recebe 2 listas
    retorna 1 dicionario
    """ 
    return dict(zip(colunas, elemento))

def arredonda(elemento):
    """
    recebe uma tupla
    retorna uma tupla com valor arredondado
    """
    chave, mm = elemento 
    return (chave, round(mm, 1))


def texto_para_lista(elemento, delimitador=','):
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

def chave_uf_ano_mes_de_lista(elemento):
    """
    recebe uma lista de elementos
    retorna uma tupla contendo uma chave e o valor em mm
    ('UF-ano-mes', 1.3)
    ['2016-01-24', '4.2', 'TO']
    """

    data, mm, uf = elemento 
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

chuva = (
    pipeline
    | "Leitura do dataset de chuvas" >> 
        ReadFromText('db.csv', skip_header_lines = 1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "Criando a chave UF-ANO-MES" >> beam.Map(chave_uf_ano_mes_de_lista)
    |  beam.CombinePerKey(sum)
    | "Arredondar casos de chuva" >> beam.Map(arredonda)
    | "Mostrar resultados" >> beam.Map(print)
 )

pipeline.run()