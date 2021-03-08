
"""A streaming Hello Beam workflow.
"""


from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount_with_metrics import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output',
      required=True,
      help=(
          'Output PubSub topic of the form '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=(
          'Input PubSub topic of the form '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
  
  #print(parser.parse_known_args(argv))
  known_args, pipeline_args = parser.parse_known_args(argv)


  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True

  p = beam.Pipeline(options=pipeline_options)

  records = (p | 'Read from PubSub' >> beam.io.ReadFromPubSub(
        topic=known_args.input_topic) | 'Write to a file' >> beam.io.WriteToText(
            known_args.output))
  
  
  

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
