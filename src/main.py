import re
import pandas as pd
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import Map

pipeline_opt = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_opt)

columns = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude'
]

def list_to_dictionary(element, columns):
    """receives a list and converts 
    it into an acceptable dictionary"""
    return dict(zip(columns, element))

def text_to_list(element, delimeter='|'):
    """receives the text and a delimiter 
    and convert in an acceptable list"""
    return element.split(delimeter)

def date_year_month(element):
    """receive a dictionary and create 
    one new column year-month"""
    element['ano-mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element

def key_uf(element):
    """receives a dictionary and return one tuple"""
    key = element['uf']
    return (
        key, element
    )

def cases_dengue(element):
    """Receives a tuple in the format ('RS', [{}, {}]
    and returns a tuple in the format ('RS-2014-12', 8.0)"""
    uf, registros = element
    for record in registros:
        if bool(re.search(r'\d', record['casos'])):
            yield (f"{uf}-{record['ano-mes']}", float(record['casos']))
        else:
            yield (f"{uf}-{record['ano-mes']}", 0.0)
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText(
        './basedb/casos_dengue.txt', skip_header_lines=1)
    | "text to list" >> beam.Map(text_to_list)
    | "Of list to dictionary" >> beam.Map(list_to_dictionary, columns)
    | "Create column year-month" >> beam.Map(date_year_month)
    | "Create the key by state" >> beam.Map(key_uf)
    | "Group by state" >> beam.GroupByKey()
    | "Descompact cases of dengue" >> beam.FlatMap(cases_dengue)
    | "Sum of dengue cases" >> beam.CombinePerKey(sum)
    | "results: " >> beam.Map(print)
)

pipeline.run()
