"""

Most basic exmaple of how Apache Beam works.

Takes JSON data stored in-memory and returns the length of each row.

https://beam.apache.org/documentation/programming-guide/
https://towardsdatascience.com/hands-on-apache-beam-building-data-pipelines-in-python-6548898b66a5

"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


with beam.Pipeline() as pipeline:

    # | means apply
    # >> allows you to name a step

    # Get data from in-memory
    myRawData = pipeline | beam.Create(
        [
            {'userId': 1, 'userName': 'Dan', 'numOrders': 4},
            {'userId': 2, 'userName': 'Joe', 'numOrders': 6},
            {'userId': 3, 'userName': 'Harry', 'numOrders': 12},
            {'userId': 4, 'userName': 'Andy', 'numOrders': 5},
        ]
    )

    # To apply a ParDo, need to create a DoFn object. This is a user-defined processing function
    # DoFn's are often the most important bit of code; they are the actual data processing tasks
    # Don't need to manually extract the elements as input; Beam does this automatically
    # Can use a Map function if it produces exactly one output element per input element
    # Is possible to include side inputs to ParDos for when input has to be determined at runtime, not hard-coded
    class ComputeLengthFn(beam.DoFn):
        def process(self, element):
            return [len(element)]

    myDataLengths = myRawData | 'GetLength' >> beam.ParDo(ComputeLengthFn())

    # Output PCollection made above
    myDataLengths | beam.io.WriteToText('/Users/daniellambert/PycharmProjects/data-engineering/outputs/',
                                 file_name_suffix='.csv')