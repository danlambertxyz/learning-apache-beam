"""

Apache Beam - Batch JSON to CSV - Dan Lambert

This script uses Apache Beam to pull a JSON file from a Google Cloud Storage bucket, manipulate the data in the file,
and output it as a CSV file in the same bucket.

Based on code written by Analytics Vidhya:
medium.com/analytics-vidhya/transform-json-to-csv-from-google-bucket-using-a-dataflow-python-pipeline-92906c87cc96

"""
from google.cloud import storage
import pandas as pd
from smart_open import open
import apache_beam as beam
import json
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
import logging

# Define these yourself
input_file = 'gs://london-weather/london_weather2021-07-26_15:00:03.json'
output_bucket = 'london-weather'

# Use 'with' to run the pipeline because it closes everything behind you automatically
with beam.Pipeline() as pipeline:

    # This will be the first step in the pipeline- reading the input file
    class ReadFile(beam.DoFn):

        def __init__(self, input_path):
            self.input_path = input_path

        def start_bundle(self):
            self.client = storage.Client()

        def process(self, something):
            new_data = []
            with open(self.input_path) as fin:
                for line in fin:
                    data = json.loads(line)

                    name = str(data.get('name'))
                    coord = data.get('coord')
                    timezone = data.get('timezone')

                    new_data.append([name, coord, timezone])

            yield new_data

    # Second step in the pipeline- writing to the output file
    class WriteCSVFile(beam.DoFn):

        def __init__(self, bucket_name):
            self.bucket_name = output_bucket

        def start_bundle(self):
            self.client = storage.Client()

        def process(self, mylist):
            df = pd.DataFrame(mylist, columns={'name': str, 'coord': str, 'timezone': int})

            bucket = self.client.get_bucket(self.bucket_name)
            bucket.blob(f"csv_lw.csv").upload_from_string(df.to_csv(index=False), 'text/csv')

    # Set some options so we can control things using the CLI tags
    class DataflowOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument('--input_path', type=str, default=input_file)
            parser.add_argument('--output_bucket', type=str, default=output_bucket)

    # Here's the actual pipeline
    def run(argv=None):
        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args(argv)

        pipeline_options = PipelineOptions(pipeline_args)
        dataflow_options = pipeline_options.view_as(DataflowOptions)

        with beam.Pipeline(options=pipeline_options) as pipeline:
            (pipeline
             | 'Start' >> beam.Create([None])
             | 'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path))
             | 'Write CSV' >> beam.ParDo(WriteCSVFile(dataflow_options.output_bucket))
             )

    if __name__ == '__main__':
        logging.getLogger().setLevel(logging.INFO)
        run()
