import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_opt = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_opt)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText(
        'casos_dengue.txt', skip_header_lines=1)
)