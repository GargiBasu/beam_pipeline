import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

"""
beam.Pipeline(options=PipelineOptions())- creates a beam pipeline by taking in the configuration options.
beam.Create- creates a PCollection from the data.
beam.FlatMap- applies to each element of the PCollection and tokenize each line into words.
beam.Map- transformation operation that maps each word to (word,1)
beam.CombinePerKey- similar to groupbykey operation and sums up each word count.
beam.ParDo- another transformation operation which is applied on key-value pair and prints the final result.
"""


with beam.Pipeline(options=PipelineOptions()) as p:
    lines = p | 'Creating PCollection' >> beam.Create(['Hello', 'Hello Good Morning', 'GoodBye'])
    
    counts = (
        lines
        | 'Tokenizing' >> beam.FlatMap(lambda x: x.split(' '))
                      
        | 'Pairing With One' >> beam.Map(lambda x: (x, 1))
        | 'GroupbyKey And Sum' >> beam.CombinePerKey(sum)
     #| 'Printing' >> beam.ParDo(lambda x: print(x[0], x[1]))
     )

    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    #Test the pipeline 
    
    assert_that(
        output,
        equal_to(["Hello: 2","Good: 1", "Morning: 1", "GoodBye: 1"]))