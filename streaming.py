import argparse
import json
import redis
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def parse_json_message(message: str) -> Dict[str, Any]:
    record = json.loads(message)
    return record

def run(
    clicks_input_subscription: str,
    checkout_input_subscription: str,
    beam_args: List[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        clicks = (
            pipeline
            | "Read from Pub/Sub - Clicks"
            >> beam.io.ReadFromPubSub(
                subscription=clicks_input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to string - Clicks" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages - Clicks" >> beam.Map(parse_json_message)
            | "Clicks:Map to key-value" >> beam.Map(lambda element: (element["user_id"],element))
            | "SessionWindows - Clicks"
            >> beam.WindowInto(
                beam.window.Sessions(3600),
                trigger=beam.trigger.Repeatedly(
                    beam.trigger.AfterProcessingTime(delay=5)
                ),
                accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
            )
        )

        checkout = (
            pipeline
            | "Read from Pub/Sub - Checkout"
            >> beam.io.ReadFromPubSub(
                subscription=checkout_input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to string - Checkout" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages - Checkout" >> beam.Map(parse_json_message)
            | "Checkout:Map to key-value" >> beam.Map(lambda element: (element["user_id"],element))
            | "SessionWindows - Checkout"
            >> beam.WindowInto(
                beam.window.Sessions(3600),
                trigger=beam.trigger.Repeatedly(
                    beam.trigger.AfterProcessingTime(delay=5)
                ),
                accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
            )
        )

        checkout_click = (
            ({"checkout":checkout,"clicks":clicks})
            | "checkout_click: Merge" >> beam.CoGroupByKey()
            | "Filter checkout" >> beam.Filter(lambda element: len(element[1]["checkout"])>0)
            | "Print" >> beam.Map(print)
        )

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--output_table",
    #     help="Output BigQuery table for results specified as: "
    #     "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    # )
    parser.add_argument(
        "--clicks_input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--checkout_input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    # parser.add_argument(
    #     "--role_id",
    #     default=60,
    #     help="Role ID",
    # )
    # parser.add_argument(
    #     "--secret_id",
    #     default=60,
    #     help="Secret ID",
    # )
    args, beam_args = parser.parse_known_args()

    run(
        clicks_input_subscription=args.clicks_input_subscription,
        checkout_input_subscription=args.checkout_input_subscription,
        beam_args=beam_args,
    )