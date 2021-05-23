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

def key_uf_year_month(element):
    """Receives a list of elements and returns a 
    tuple containing a key and a value
    ('UF-YAER-MONTH', 1.3)"""
    date, mm, uf = element
    yaer_month = '-'.join(date.split('-')[:2])
    key = f'{uf}-{yaer_month}'

    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return key, mm

def round_results(elements):
    """Rounding rain results
    Receives a tuple and returns a tuple with rounded values
    """
    key, mm = elements
    return (key, round(mm, 1))

def filter_fields(element):
    """
    Removes tuples where it only has a specific 
    value for example rain: [''], dengue: 85.0
    """
    key, dados = element

    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    
    return False

dengue = (
    pipeline
    | "Reading the dengue dataset" >> ReadFromText(
        './basedb/sample_casos_dengue.txt', skip_header_lines=1)
    | "text to list" >> beam.Map(text_to_list)
    | "Of list to dictionary" >> beam.Map(list_to_dictionary, columns)
    | "Create column year-month" >> beam.Map(date_year_month)
    | "Create the key by state" >> beam.Map(key_uf)
    | "Group by state" >> beam.GroupByKey()
    | "Descompact cases of dengue" >> beam.FlatMap(cases_dengue)
    | "Sum of dengue cases" >> beam.CombinePerKey(sum)
    #| "results: " >> beam.Map(print)
)


rain = (
    pipeline
    | "Reading the rainfall dataset" >>
        ReadFromText('./basedb/sample_chuvas.csv', skip_header_lines=1)
    | "Of text to list (rainfall)" >> beam.Map(text_to_list, delimeter=',')
    | "Create key uf-ano-mes" >> beam.Map(key_uf_year_month)
    | "Sum of rainfall cases" >> beam.CombinePerKey(sum)
    | "Rounding rain results" >> beam.Map(round_results)
    #| "Show rain results" >> beam.Map(print)
)

results = (
    #(rain, dengue)
    #| "Join at pcollections dengue and rainfall's" >> beam.Flatten()
    #| "Agroup" >> beam.GroupByKey()
    #| "Show results finaly" >> beam.Map(print)
    ({'chuvas': rain, 'dengue': dengue})
    | "Merge at results" >> beam.CoGroupByKey()
    | "Filter data" >> beam.Filter(filter_fields)
    | "Show results finaly" >> beam.Map(print)
)

pipeline.run()
