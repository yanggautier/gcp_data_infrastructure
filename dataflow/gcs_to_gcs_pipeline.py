import apache_beam as beam
import argparse
import csv
import os
from datetime import datetime
import apache_beam.io.fileio as fileio
from apache_beam.options.pipeline_options import PipelineOptions
import logging


def run(input_path, output_path, staging_path, temp_path, project_id):
    """
    Exécute le pipeline Beam pour traiter les fichiers CSV.

    Args:
        bucket_name (str): Bucket GCS source
        project_id (str): ID du projet GCP
    """

    # Configurations globales
    DATE_COLUMNS = {
        "olist_order_items_dataset.csv": {"shipping_limit_date": "%Y-%m-%d %H:%M:%S"},
        "olist_order_reviews_dataset.csv": {
            "review_creation_date": "%Y-%m-%d %H:%M:%S", 
            "review_answer_timestamp": "%Y-%m-%d %H:%M:%S"
        },
        "olist_orders_dataset.csv": {
            "order_purchase_timestamp": "%Y-%m-%d %H:%M:%S", 
            "order_approved_at": "%Y-%m-%d %H:%M:%S", 
            "order_delivered_carrier_date": "%Y-%m-%d %H:%M:%S", 
            "order_delivered_customer_date": "%Y-%m-%d %H:%M:%S", 
            "order_estimated_delivery_date": "%Y-%m-%d %H:%M:%S"
        },
    }

    DUPLICATE = {
        "olist_geolocation_dataset.csv": "geolocation_zip_code_prefix",
        "olist_order_reviews_dataset.csv": "review_id"
    }
    pipeline_options = PipelineOptions([
        '--runner=DataflowRunner',
        f'--project={project_id}',
        '--region=europe-west9',
        f'--staging_location={staging_path}',
        f'--temp_location={temp_path}',
        '--service_account_email=accountservicedataflow@vibrant-epsilon-454713-m2.iam.gserviceaccount.com'
    ])
    import apache_beam as beam

    # Fonction de transformation des dates
    def transform_dates(row, file_name):
        if file_name in DATE_COLUMNS:
            for col, fmt in DATE_COLUMNS[file_name].items():
                if col in row and row[col]:  # Vérifier si la colonne existe et n'est pas vide
                    try:
                        row[col] = datetime.strptime(row[col], fmt).isoformat()
                    except ValueError:
                        pass  # En cas d'erreur, on laisse la valeur inchangée
        return row
    
    # Fonction de lecture CSV
    def read_csv(file_name, content):
        import csv
        import io

        if isinstance(content, bytes):
            content = content.decode('utf-8')
        
        reader = csv.DictReader(io.StringIO(content))
        return [dict(row) for row in reader], file_name

    def save_csv_file(input_tuple, output_path):
        # Logging for debugging
        import csv
        logging.info(f"Processing input: {input_tuple}")
        
        # Ensure input_tuple is not None and has expected structure
        if input_tuple is None or not isinstance(input_tuple, tuple):
            logging.warning(f"Received invalid input: {input_tuple}")
            return
        
        rows, file_path = input_tuple
        file_name = file_path.split("/")[-1]
        
        # Ignorer les fichiers vides
        if not rows:
            logging.warning(f"No rows in file {file_name}")
            return

        # Créer le répertoire de sortie s'il n'existe pas
        os.makedirs(output_path, exist_ok=True)

        # Chemin complet du fichier
        output_file = os.path.join(output_path, file_name)

        # Écriture du fichier CSV
        with open(output_file, 'w', newline='') as csvfile:
            # Vérifier qu'il y a des données
            if rows:
                # Créer un writer CSV
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                
                # Écrire les en-têtes
                writer.writeheader()
                
                # Écrire les données
                writer.writerows(rows)
        
        logging.info(f"Saved file: {output_file}")
        
    class WriteToGCS(beam.DoFn):
        def __init__(self, output_path):
            self.output_path = output_path

        def process(self, element):
            from apache_beam.io.filesystems import FileSystems
            import io
            import csv
            
            rows, file_path = element
            file_name = file_path.split("/")[-1]

            if not rows:
                return  # Ignore les fichiers vides

            # Construire le chemin complet du fichier GCS
            file_path = f"{self.output_path}/{file_name}"
            
            # Convertir les données en format CSV
            csv_content = io.StringIO()
            writer = csv.DictWriter(csv_content, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

            # Écriture dans GCS
            with FileSystems.create(file_path) as f:
                f.write(csv_content.getvalue().encode("utf-8"))
            
            logging.info(f"✅ Fichier écrit sur GCS: {file_path}")

            yield file_path  # Retourne le chemin pour debug

        # Pipeline Apache Beam
    with beam.Pipeline(options=pipeline_options) as p:
        files = (
            p
            | "Match Files" >> fileio.MatchFiles(f"{input_path}/*.csv")
            | "Read Matches" >> fileio.ReadMatches()  # Lire tous les fichiers trouvés
            | "Extract Content" >> beam.Map(lambda file: (file.metadata.path, file.read_utf8()))
            | "Parse CSV" >> beam.MapTuple(read_csv)
            | "Transform Rows" >> beam.MapTuple(lambda rows, file_name: ([transform_dates(row, file_name) for row in rows], file_name))
            | "Size" >> beam.Filter(lambda x: x is not None)
        )

        # Gestion des duplicatas
        deduped_files = (
            files
            | "Filter Duplicates" >> beam.Filter(lambda x: x[1] in DUPLICATE)
            | "Apply Deduplication" >> beam.Map(lambda x: (list({row[DUPLICATE[x[1]]]: row for row in x[0]}.values()), x[1]))
        )

        not_duplicated_files = (
            files
            | "Filter Non-Duplicates" >> beam.Filter(lambda x: x[1] not in DUPLICATE)
        )

        combined_files = (
            (deduped_files, not_duplicated_files)
            | "Flatten PCollections" >> beam.Flatten()
        )

        (
            combined_files
            | "Write Files to GCS" >> beam.ParDo(WriteToGCS(output_path))
        )

        #combined_files | "Save CSV Files" >> beam.Map(lambda x: save_csv_file(x, output_path) if x is not None else None)


def main():
    parser = argparse.ArgumentParser(description='Pipeline de traitement CSV avec Apache Beam')
    # Arguments pour Beam
    parser.add_argument('--runner', required=True, help='Type de runner (ex: DataflowRunner)')
    parser.add_argument('--project', required=True, help='ID du projet GCP')
    parser.add_argument('--region', required=True, help='Région GCP')
    parser.add_argument('--staging_location', required=True, help='Bucket GCS pour staging')
    parser.add_argument('--temp_location', required=True, help='Bucket GCS pour temp')

    # Arguments spécifiques au pipeline
    parser.add_argument('--input_bucket', required=True, help='Bucket GCS source')
    parser.add_argument('--output_bucket', required=True, help='Bucket GCS source')

    args, beam_args = parser.parse_known_args() 

    args = parser.parse_args()
    run(args.input_bucket, args.output_bucket, args.staging_location, args.temp_location, args.project)


if __name__ == '__main__':
    main()
