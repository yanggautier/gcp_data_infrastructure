import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

# Mapping des colonnes par table
SCHEMA_MAPPING = {
    "customer": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "geolocalisation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"],
    "category_name": ["product_category_name", "product_category_name_english"],
    "product": ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", 
                "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
    "seller": ["seller_id", "seller_zip_code_prefix"],
    "online_order": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", 
                     "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
    "order_item": ["id", "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
    "order_payment": ["id", "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    "order_review": ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", 
                     "review_creation_date", "review_answer_timestamp"]
}

class ParseCSV(beam.DoFn):
    def __init__(self, table_name):
        self.table_name = table_name

    def process(self, line):
        values = line.split(",")  # Séparer les colonnes du CSV
        columns = SCHEMA_MAPPING[self.table_name]  # Obtenir les colonnes définies
        if len(values) != len(columns):
            return []  # Ignorer les lignes corrompues
        return [dict(zip(columns, values))]  # Associer les valeurs aux noms des colonnes

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help="Chemin du fichier CSV dans GCS")
    parser.add_argument('--output', required=True, help="Nom de la table BigQuery")
    parser.add_argument('--table_name', required=True, help="Nom de la table pour mapper les colonnes")
    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',
        project='vibrant-epsilon-454713-m2',
        region='europe-west9'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(known_args.input)  # Pas de header
            | 'Parse CSV' >> beam.ParDo(ParseCSV(known_args.table_name))  # Mapper colonnes -> valeurs
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                known_args.output,
                schema='SCHEMA_AUTODETECT',  # Optionnel, peut être remplacé par un JSON de schéma
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

if __name__ == '__main__':
    run()
