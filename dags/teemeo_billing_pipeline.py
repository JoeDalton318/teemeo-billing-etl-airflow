"""
DAG Airflow principal pour le pipeline ETL de facturation médicale Teemeo.

Ce DAG orchestre l'ensemble du processus ETL:
1. Extract: Lecture et upload vers MinIO bronze
2. Transform: Standardisation et sauvegarde vers MinIO silver
3. Validate: Contrôle qualité des données
4. Load: Chargement dans PostgreSQL (staging → dimensions → faits)
5. ML: Détection d'anomalies avec IsolationForest
6. Datamart: Création du datamart pour la relance des créances

Auteur: Data Engineering Team
Date: 2026-02-12
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Ajouter le répertoire scripts au PYTHONPATH pour importer etl_tasks
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

# Import des fonctions ETL
from etl_tasks import (
    extract_files_to_bronze,
    transform_to_silver,
    validate_quality,
    load_to_staging,
    build_dimensions,
    build_fact_table,
    detect_anomalies,
    create_datamart_relance
)

# ============================================================================
# CONFIGURATION DU DAG
# ============================================================================

# Arguments par défaut appliqués à toutes les tâches
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,  # Ne dépend pas de l'exécution précédente
    'email': ['data-team@teemeo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,  # Nombre de tentatives en cas d'échec
    'retry_delay': timedelta(minutes=5),  # Délai entre les tentatives
    'execution_timeout': timedelta(hours=2),  # Timeout global
}

# ============================================================================
# DÉFINITION DU DAG
# ============================================================================

dag = DAG(
    dag_id='teemeo_billing_pipeline',
    default_args=default_args,
    description='Pipeline ETL complet pour la facturation médicale - Architecture Medallion + Star Schema + ML',
    schedule_interval='@daily',  # Exécution quotidienne à minuit
    # schedule_interval='0 3 * * *',  # Alternative: tous les jours à 3h du matin
    start_date=days_ago(1),  # Commence hier pour permettre l'exécution immédiate
    catchup=False,  # Ne pas rattraper les exécutions passées
    max_active_runs=1,  # Une seule exécution à la fois
    tags=['etl', 'billing', 'healthcare', 'teemeo', 'ml'],
    doc_md=__doc__,
)

# ============================================================================
# TÂCHE 0: Initialisation (optionnel)
# ============================================================================

task_start = BashOperator(
    task_id='start_pipeline',
    bash_command='''
    echo "============================================================"
    echo "Démarrage du pipeline Teemeo Billing ETL"
    echo "============================================================"
    echo "Date d'exécution: {{ ds }}"
    echo "Timestamp: {{ ts }}"
    echo "DAG Run ID: {{ run_id }}"
    echo "============================================================"
    ''',
    dag=dag,
)

# ============================================================================
# PHASE 1: EXTRACT
# ============================================================================

task_extract = PythonOperator(
    task_id='extract_to_bronze',
    python_callable=extract_files_to_bronze,
    provide_context=True,
    doc_md="""
    ## Extract - Lecture des sources
    
    Cette tâche:
    - Liste tous les fichiers CSV/Excel dans data/raw/
    - Upload vers MinIO bucket `bronze` (data lake brut)
    - Conserve l'immutabilité des sources
    - Pousse la liste des fichiers vers XCom
    
    **Bronze layer**: Données exactly as-is, pas de transformation.
    """,
    dag=dag,
)

# ============================================================================
# PHASE 2: TRANSFORM
# ============================================================================

task_transform = PythonOperator(
    task_id='transform_to_silver',
    python_callable=transform_to_silver,
    provide_context=True,
    doc_md="""
    ## Transform - Standardisation
    
    Cette tâche:
    - Récupère la liste des fichiers depuis XCom
    - Applique les mappings de colonnes (COLUMN_MAPPINGS)
    - Normalise les types (dates, montants)
    - Calcule les métriques dérivées (montant_impayé, délai_paiement)
    - Consolide tous les fichiers en un DataFrame unique
    - Sauvegarde en Parquet dans MinIO bucket `silver`
    
    **Silver layer**: Données nettoyées et standardisées.
    """,
    dag=dag,
)

# ============================================================================
# PHASE 3: VALIDATE
# ============================================================================

task_validate = PythonOperator(
    task_id='validate_quality',
    python_callable=validate_quality,
    provide_context=True,
    doc_md="""
    ## Validate - Contrôle qualité
    
    Checks effectués:
    - Valeurs manquantes sur champs critiques (patient_id, montant_facture)
    - Montants négatifs
    - Dates futures
    - Doublons
    - Cohérence montants (montant_payé ≤ montant_facturé)
    - Cohérence dates (date_paiement ≥ date_facturation)
    
    **Comportement**: Échec du DAG si problèmes critiques détectés.
    """,
    dag=dag,
)

# ============================================================================
# PHASE 4: LOAD - Staging
# ============================================================================

task_load_staging = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    provide_context=True,
    doc_md="""
    ## Load Staging
    
    Charge les données dans la table `staging.facturation`.
    
    Zone tampon permettant:
    - Validation métier avant intégration au DW
    - Rejouabilité en cas d'erreur
    - Traçabilité (source_file, load_timestamp)
    """,
    dag=dag,
)

# ============================================================================
# PHASE 5: BUILD - Dimensions
# ============================================================================

task_build_dimensions = PythonOperator(
    task_id='build_dimensions',
    python_callable=build_dimensions,
    provide_context=True,
    doc_md="""
    ## Build Dimensions
    
    Construit/met à jour les tables de dimensions:
    - `dw.dim_patient` (surrogate key: patient_key)
    - `dw.dim_etablissement` (surrogate key: etablissement_key)
    - `dw.dim_date` (surrogate key: date_key)
    - `dw.dim_service` (surrogate key: service_key)
    
    Utilise INSERT...ON CONFLICT DO NOTHING pour éviter les doublons.
    
    **Schéma en étoile**: Dimensions avec clés de substitution.
    """,
    dag=dag,
)

# ============================================================================
# PHASE 6: BUILD - Fact Table
# ============================================================================

task_build_fact = PythonOperator(
    task_id='build_fact_table',
    python_callable=build_fact_table,
    provide_context=True,
    doc_md="""
    ## Build Fact Table
    
    Construit la table de faits `dw.fact_facturation`.
    
    Jointures:
    - staging.facturation (données sources)
    - dw.dim_patient (récupère patient_key)
    - dw.dim_etablissement (récupère etablissement_key)
    - dw.dim_date (récupère date_key)
    - dw.dim_service (récupère service_key)
    
    **Granularité**: Une ligne = une facture.
    **Mesures**: montant_facture, montant_payé, montant_impayé, délai_paiement.
    """,
    dag=dag,
)

# ============================================================================
# PHASE 7: MACHINE LEARNING - Détection d'anomalies
# ============================================================================

task_detect_anomalies = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_anomalies,
    provide_context=True,
    doc_md="""
    ## ML - Détection d'anomalies
    
    Utilise **IsolationForest** (scikit-learn) pour détecter les créances anormales.
    
    **Features**:
    - montant_impayé
    - délai_paiement_jours
    - nombre_lignes (complexité)
    
    **Méthode**:
    1. Normalisation avec StandardScaler
    2. IsolationForest (contamination=5%)
    3. Prédiction: -1 (anomalie) ou 1 (normal)
    4. Mise à jour de la colonne anomaly_score
    
    **Cas d'usage**: Prioriser les relances sur les créances suspectes.
    """,
    dag=dag,
)

# ============================================================================
# PHASE 8: DATAMART - Création datamart relance
# ============================================================================

task_create_datamart = PythonOperator(
    task_id='create_datamart_relance',
    python_callable=create_datamart_relance,
    provide_context=True,
    doc_md="""
    ## Datamart - Relance des créances
    
    Crée une vue matérialisée `datamart.relance` pour les équipes de recouvrement.
    
    **Critères**:
    - Créances impayées (montant_impayé > 0)
    - Délai > 30 jours
    
    **Priorités**:
    - URGENT: anomaly_score = -1
    - PRIORITAIRE: montant_impayé > 5000 €
    - IMPORTANT: délai > 60 jours
    - STANDARD: autres
    
    **Usage métier**: SELECT * FROM datamart.relance WHERE priorite='URGENT'
    """,
    dag=dag,
)

# ============================================================================
# TÂCHE FINALE: Résumé et statistiques
# ============================================================================

task_summary = BashOperator(
    task_id='pipeline_summary',
    bash_command='''
    echo "============================================================"
    echo " Pipeline Teemeo Billing ETL terminé avec succès"
    echo "============================================================"
    echo ""
    echo " Statistiques disponibles dans:"
    echo "   - dw.v_facturation_complete (vue complète)"
    echo "   - dw.v_stats_etablissement (stats par établissement)"
    echo "   - dw.v_ca_mensuel (CA mensuel + taux recouvrement)"
    echo "   - datamart.relance (créances à relancer)"
    echo ""
    echo " Accès aux données:"
    echo "   - Adminer: http://localhost:9080"
    echo "   - MinIO Console: http://localhost:9001"
    echo ""
    echo "============================================================"
    ''',
    dag=dag,
)

# ============================================================================
# DÉFINITION DES DÉPENDANCES (WORKFLOW)
# ============================================================================

# Syntaxe chainée (lisible)
task_start >> task_extract >> task_transform >> task_validate >> task_load_staging

# Le staging alimente les dimensions
task_load_staging >> task_build_dimensions

# Les dimensions doivent être prêtes avant la table de faits
task_build_dimensions >> task_build_fact

# La détection d'anomalies nécessite la table de faits
task_build_fact >> task_detect_anomalies

# Le datamart dépend des anomalies
task_detect_anomalies >> task_create_datamart

# Résumé final
task_create_datamart >> task_summary

# ============================================================================
# VISUALISATION RECOMMANDÉE (Graph View dans Airflow UI)
# ============================================================================
"""
Graph View:

    start_pipeline
         ↓
    extract_to_bronze
         ↓
    transform_to_silver
         ↓
    validate_quality
         ↓
    load_to_staging
         ↓
    build_dimensions
         ↓
    build_fact_table
         ↓
    detect_anomalies
         ↓
    create_datamart_relance
         ↓
    pipeline_summary
"""

# ============================================================================
# NOTES POUR L'ENTRETIEN
# ============================================================================
"""
Points clés à mentionner:

1. **Architecture Medallion** (Bronze → Silver → Gold)
   - Bronze: MinIO, données brutes
   - Silver: MinIO, Parquet standardisés
   - Gold: PostgreSQL, schéma en étoile + datamarts

2. **Schéma en étoile**
   - Dimensions avec surrogate keys
   - Table de faits centrale
   - Requêtes BI optimisées

3. **Gestion d'erreurs**
   - Retry automatique (2 fois, 5 min)
   - Validation bloquante
   - Logs détaillés

4. **Orchestration Airflow**
   - DAG as code en Python
   - XCom pour passage de données entre tâches
   - Dependencies avec >>
   - Schedule quotidien

5. **Volet IA**
   - IsolationForest pour anomalies
   - Features: montant, délai, complexité
   - Intégration dans le datamart métier

6. **Migration AWS**
   - MinIO → S3
   - Airflow → MWAA
   - PostgreSQL → RDS/Redshift
   - Scripts Python → Lambda (optionnel)
"""
