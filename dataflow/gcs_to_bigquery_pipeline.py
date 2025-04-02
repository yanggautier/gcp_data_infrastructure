import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

# Mapping des colonnes par table
SCHEMA_MAPPING = {
    "customer": [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_unique_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_zip_code_prefix", "type": "INTEGER"},
        {"name": "customer_city", "type": "STRING"},
        {"name": "customer_state", "type": "STRING"}
    ],
    "geolocalisation": [
        {"name": "geolocation_zip_code_prefix", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "geolocation_lat", "type": "FLOAT"},
        {"name": "geolocation_lng", "type": "FLOAT"},
        {"name": "geolocation_city", "type": "STRING"},
        {"name": "geolocation_state", "type": "STRING"}
    ],
    "category_name": [
        {"name": "product_category_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_category_name_english", "type": "STRING"}
    ],
    "product": [
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_category_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name_lenght", "type": "INTEGER"},
        {"name": "product_description_lenght", "type": "INTEGER"},
        {"name": "product_photos_qty", "type": "INTEGER"},
        {"name": "product_weight_g", "type": "FLOAT"},
        {"name": "product_length_cm", "type": "FLOAT"},
        {"name": "product_height_cm", "type": "FLOAT"},
        {"name": "product_width_cm", "type": "FLOAT"}
    ],
    "seller": [
        {"name": "seller_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "seller_zip_code_prefix", "type": "INTEGER"}
    ],
    "online_order": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_status", "type": "STRING"},
        {"name": "order_purchase_timestamp", "type": "STRING"},
        {"name": "order_approved_at", "type": "TIMESTAMP"},
        {"name": "order_delivered_carrier_date", "type": "STRING"},
        {"name": "order_delivered_customer_date", "type": "STRING"},
        {"name": "order_estimated_delivery_date", "type": "STRING"}
    ],
    "order_item": [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_item_id", "type": "INTEGER"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "seller_id", "type": "STRING"},
        {"name": "shipping_limit_date", "type": "STRING"},
        {"name": "price", "type": "FLOAT"},
        {"name": "freight_value", "type": "FLOAT"}
    ],
    "order_payment": [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "payment_sequential", "type": "INTEGER"},
        {"name": "payment_type", "type": "STRING"},
        {"name": "payment_installments", "type": "INTEGER"},
        {"name": "payment_value", "type": "FLOAT"}
    ]
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
    parser.add_argument('--project', required=True, help="Id du projet")
    parser.add_argument('--region', required=True, help="Region du service cloud")
    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',
        project=known_args.project,
        region=known_args.region  
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
