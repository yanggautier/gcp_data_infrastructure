# README - Data Pipeline avec Apache Beam, Dataflow et Cloud Composer

## 📌 Description du projet
Ce projet implémente une architecture de pipeline de données sur Google Cloud Platform (GCP) pour traiter et charger des fichiers CSV dans BigQuery. Il utilise **Apache Beam**, **Dataflow**, et **Cloud Composer (Airflow)** pour orchestrer le traitement et le chargement des données.

## 📂 Arborescence du projet
```
.
├── Data dictionary.xlsx          # Dictionnaire des données
├── README.md                     # Documentation du projet
├── cloud_composer                # Contient les DAGs pour orchestrer les pipelines Dataflow
│   └── dags
│       ├── composer_dataflow_dag.py  # DAG pour exécuter le pipeline GCS → BigQuery
│       └── gsc_to_bigquery_dag.py    # DAG supplémentaire pour ingestion
├── dataflow                       # Contient les pipelines Apache Beam
│   ├── gcs_to_bigquery_pipeline.py  # Pipeline Beam pour charger les données dans BigQuery
│   └── gcs_to_gcs_pipeline.py       # Pipeline Beam pour transformer et stocker les données dans GCS
├── notebooks                      # Analyses exploratoires et tests
├── sqls                           # Scripts SQL pour BigQuery et Cloud SQL
└── requirements.txt               # Dépendances du projet
```

## 🚀 Technologies utilisées
- **Google Cloud Platform (GCP)** : Cloud Storage, Dataflow, BigQuery, Cloud Composer
- **Apache Beam** : Framework pour traitement de flux de données
- **Google Cloud Dataflow** : Exécution des pipelines Apache Beam
- **Apache Airflow (Cloud Composer)** : Orchestration des tâches
- **Python** : Développement des pipelines

## 🔄 Fonctionnement des pipelines Apache Beam
### 1️⃣ **Pipeline GCS → GCS (Transformation des CSV) **
Fichier concerné : `gcs_to_gcs_pipeline.py`
- Lit les fichiers CSV depuis un bucket GCS
- Applique des transformations (conversion des dates, suppression des doublons)
- Sauvegarde les fichiers transformés dans un autre bucket

### 2️⃣ **Pipeline GCS → BigQuery**
Fichier concerné : `gcs_to_bigquery_pipeline.py`
- Lit les fichiers CSV depuis GCS
- Nettoie et transforme les données en fonction d’un schéma
- Charge les données dans des tables BigQuery

### 3️⃣ **Orchestration avec Airflow (Cloud Composer)**
DAG concerné : `composer_dataflow_dag.py`
- Déclenche le pipeline Dataflow `gcs_to_bigquery_pipeline.py`
- Utilise `DataflowCreatePythonJobOperator` pour soumettre le job
- Exécute le pipeline pour chaque table définie dans BigQuery

## 📌 Instructions d'exécution
### 1️⃣ **Déploiement du pipeline Apache Beam sur Dataflow**
```sh
python dataflow/gcs_to_gcs_pipeline.py \
  --runner=DataflowRunner \
  --project=your-gcp-project \
  --region=europe-west9 \
  --staging_location=gs://your-bucket/staging/ \
  --temp_location=gs://your-bucket/temp/ \
  --input_bucket=gs://your-input-bucket \
  --output_bucket=gs://your-output-bucket
```

### 2️⃣ **Exécution du DAG Airflow dans Cloud Composer**
1. Se connecter à l’interface Airflow de Cloud Composer
2. Activer et exécuter le DAG `composer_dataflow_dag`
3. Vérifier l’état des tâches et les logs dans Airflow


## 📌 Améliorations possibles
- Automatiser la détection des schémas pour BigQuery
- Ajouter des tests unitaires pour les transformations de données
- Optimiser le pipeline pour supporter des volumes de données plus importants

---

📢 **Auteur** : Projet conçu pour orchestrer et transformer des données e-commerce avec GCP 🚀

