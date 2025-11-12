# dataflow/beam_transform.py
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

BQ_SCHEMA = {
    "fields": [
        {"name": "fetched_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "asset", "type": "STRING", "mode": "REQUIRED"},
        {"name": "price_usd", "type": "FLOAT", "mode": "NULLABLE"}
    ]
}

class ParseJsonDoFn(beam.DoFn):
    def process(self, element):
        # element is a line/JSON string
        row = json.loads(element)
        fetched_at = row.get("fetched_at")
        data = row.get("data", {})
        # example for coinGecko structure
        for asset, vals in data.items():
            yield {
                "fetched_at": fetched_at,
                "asset": asset,
                "price_usd": float(vals.get("usd")) if vals.get("usd") is not None else None
            }

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS path to raw JSON (can include {{ds}} pattern)")
    parser.add_argument("--output_table", required=True, help="PROJECT:DATASET.TABLE")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    options.view_as(StandardOptions).runner = pipeline_args and pipeline_args[-1] or options.view_as(StandardOptions).runner

    with beam.Pipeline(options=options) as p:
        (p
         | "ReadRawJson" >> beam.io.ReadFromText(known_args.input)
         | "ParseJson" >> beam.ParDo(ParseJsonDoFn())
         | "WriteToBQ" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            ))

if __name__ == "__main__":
    run()
