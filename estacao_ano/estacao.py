from sys import argv
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_estacao =[ 
                "estacao",
                "imagem",
                "nome",
                "tipo"] 

def lista_para_dicionario(elemento, colunas):
    """
    recebe 2 listas
    retorna 1 dicionario
    """
    return dict(zip(colunas, elemento))


def texto_para_lista(elemento, delimitador=','):
    """
    Recebe um texto e um demilitador  
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def chave_uf(elemento):

    chave = elemento['estacao']

    return (chave, elemento)

dengue = (
    pipeline
    | "Leitura do dataset" >> 
        ReadFromText('lista.txt', skip_header_lines = 1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_estacao)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pela estacao" >> beam.GroupByKey()
    | "Mostrar resultados" >> beam.Map(print)
 )

pipeline.run()