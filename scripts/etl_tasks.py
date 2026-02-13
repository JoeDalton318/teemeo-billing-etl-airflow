"""
Module contenant toutes les fonctions ETL pour le pipeline de facturation médicale.

Fonctions principales:
- extract: Lecture des fichiers sources
- transform: Standardisation et nettoyage
- validate: Contrôle qualité
- load: Chargement dans PostgreSQL
- ml: Détection d'anomalies
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
import logging
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import os

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Chemins
DATA_RAW_DIR = Path("/opt/airflow/data/raw")
DATA_PROCESSED_DIR = Path("/opt/airflow/data/processed")

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET_BRONZE = "bronze"
MINIO_BUCKET_SILVER = "silver"

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "teemeo_dw")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "teemeo_dw")
POSTGRES_DB = os.getenv("POSTGRES_DB", "teemeo_dw")

POSTGRES_CONN_STRING = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Mapping des colonnes par établissement
COLUMN_MAPPINGS = {
    "etablissement1": {
        "NumPatient": "patient_id",
        "NomPatient": "patient_nom",
        "PrenomPatient": "patient_prenom",
        "CodeEtablissement": "etablissement_id",
        "NomEtablissement": "etablissement_nom",
        "ServiceMedical": "service",
        "DateFacture": "date_facturation",
        "Montant": "montant_facture",
        "StatutPaiement": "statut_paiement",
        "MontantPaye": "montant_paye",
        "DatePaiement": "date_paiement",
        "NombreLignes": "nombre_lignes"
    },
    "etablissement2": {
        "PatientID": "patient_id",
        "LastName": "patient_nom",
        "FirstName": "patient_prenom",
        "FacilityCode": "etablissement_id",
        "FacilityName": "etablissement_nom",
        "Department": "service",
        "BillingDate": "date_facturation",
        "InvoiceAmount": "montant_facture",
        "PaymentStatus": "statut_paiement",
        "PaidAmount": "montant_paye",
        "PaymentDate": "date_paiement",
        "LineItems": "nombre_lignes"
    },
    "etablissement3": {
        "ID_Pat": "patient_id",
        "Nom": "patient_nom",
        "Prenom": "patient_prenom",
        "Etab_Code": "etablissement_id",
        "Etab_Nom": "etablissement_nom",
        "Serv": "service",
        "Dt_Fact": "date_facturation",
        "Mt_Fact": "montant_facture",
        "Statut": "statut_paiement",
        "Mt_Paye": "montant_paye",
        "Dt_Paye": "date_paiement",
        "Nb_Lignes": "nombre_lignes"
    },
    "etablissement4": {
        "ID_Patient": "patient_id",
        "NomFamille": "patient_nom",
        "Prenom": "patient_prenom",
        "Hospital_ID": "etablissement_id",
        "Hospital_Name": "etablissement_nom",
        "Service": "service",
        "Invoice_Date": "date_facturation",
        "Total_Amount": "montant_facture",
        "Status": "statut_paiement",
        "Amount_Paid": "montant_paye",
        "Payment_Date": "date_paiement",
        "Item_Count": "nombre_lignes"
    },
    "etablissement5": {
        "ref_patient": "patient_id",
        "family_name": "patient_nom",
        "given_name": "patient_prenom",
        "clinic_ref": "etablissement_id",
        "clinic_label": "etablissement_nom",
        "medical_dept": "service",
        "billing_timestamp": "date_facturation",
        "billed_amt": "montant_facture",
        "payment_state": "statut_paiement",
        "received_amt": "montant_paye",
        "received_date": "date_paiement",
        "line_count": "nombre_lignes"
    }
}


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def get_minio_client():
    """Crée et retourne un client MinIO (S3-compatible)."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version='s3v4')
    )


def get_postgres_engine():
    """Crée et retourne un engine SQLAlchemy pour PostgreSQL."""
    return create_engine(POSTGRES_CONN_STRING)


def ensure_minio_buckets():
    """Crée les buckets MinIO s'ils n'existent pas."""
    s3_client = get_minio_client()
    buckets = [MINIO_BUCKET_BRONZE, MINIO_BUCKET_SILVER]
    
    for bucket in buckets:
        try:
            s3_client.head_bucket(Bucket=bucket)
            logger.info(f" Bucket '{bucket}' existe déjà")
        except ClientError:
            s3_client.create_bucket(Bucket=bucket)
            logger.info(f" Bucket '{bucket}' créé")


# ============================================================================
# EXTRACT
# ============================================================================

def extract_files_to_bronze(**context) -> List[str]:
    """
    Tâche 1: Extrait les fichiers du répertoire data/raw/ et les upload vers MinIO bronze.
    
    Returns:
        Liste des fichiers uploadés
    """
    logger.info("=" * 70)
    logger.info("EXTRACT: Lecture des fichiers sources")
    logger.info("=" * 70)
    
    # Créer les buckets si nécessaire
    ensure_minio_buckets()
    
    s3_client = get_minio_client()
    uploaded_files = []
    
    # Lister tous les fichiers CSV et Excel
    file_patterns = ['*.csv', '*.xlsx']
    files_to_process = []
    
    for pattern in file_patterns:
        files_to_process.extend(DATA_RAW_DIR.glob(pattern))
    
    logger.info(f" Fichiers trouvés: {len(files_to_process)}")
    
    # Upload vers MinIO bronze
    for file_path in files_to_process:
        object_name = f"raw/{datetime.now().strftime('%Y%m%d')}/{file_path.name}"
        
        try:
            s3_client.upload_file(
                str(file_path),
                MINIO_BUCKET_BRONZE,
                object_name
            )
            uploaded_files.append(object_name)
            logger.info(f"Uploaded: {file_path.name} -> bronze/{object_name}")
            
        except Exception as e:
            logger.error(f"Erreur upload {file_path.name}: {e}")
            raise
    
    logger.info(f"\n{len(uploaded_files)} fichiers uploadés vers bronze")
    
    # Pousser la liste des fichiers vers XCom pour la tâche suivante
    context['ti'].xcom_push(key='bronze_files', value=[str(f) for f in files_to_process])
    
    return uploaded_files


# ============================================================================
# TRANSFORM
# ============================================================================

def standardize_file(file_path: Path) -> pd.DataFrame:
    """
    Standardise un fichier selon son établissement.
    
    Args:
        file_path: Chemin du fichier à traiter
        
    Returns:
        DataFrame standardisé
    """
    # Identifier l'établissement depuis le nom du fichier
    filename_lower = file_path.stem.lower()
    
    etab_key = None
    for key in COLUMN_MAPPINGS.keys():
        if key in filename_lower:
            etab_key = key
            break
    
    if not etab_key:
        raise ValueError(f"Impossible d'identifier l'établissement pour {file_path.name}")
    
    logger.info(f"   Traitement: {file_path.name} (mappé vers {etab_key})")
    
    # Lecture du fichier (détection auto du séparateur pour CSV)
    if file_path.suffix == '.xlsx':
        df = pd.read_excel(file_path, engine='openpyxl')
    else:
        df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
    
    logger.info(f"   → {len(df)} lignes lues")
    
    # Appliquer le mapping de colonnes
    mapping = COLUMN_MAPPINGS[etab_key]
    df = df.rename(columns=mapping)
    
    # Normalisation des types de données
    
    # Dates - essayer plusieurs formats
    date_columns = ['date_facturation', 'date_paiement']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Montants - convertir en numérique
    montant_columns = ['montant_facture', 'montant_paye']
    for col in montant_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Nombre de lignes
    if 'nombre_lignes' in df.columns:
        df['nombre_lignes'] = pd.to_numeric(df['nombre_lignes'], errors='coerce').fillna(1).astype(int)
    
    # Calcul du montant impayé
    df['montant_impaye'] = (df['montant_facture'] - df['montant_paye']).round(2)
    
    # Calcul du délai de paiement (en jours)
    df['delai_paiement_jours'] = (
        df['date_paiement'] - df['date_facturation']
    ).dt.days
    
    # Ajout des métadonnées
    df['source_file'] = file_path.name
    df['load_timestamp'] = datetime.now()
    
    logger.info(f"   Standardisation terminée")
    
    return df


def transform_to_silver(**context) -> str:
    """
    Tâche 2: Standardise tous les fichiers et les sauvegarde en Parquet dans MinIO silver.
    
    Returns:
        Chemin du fichier Parquet consolidé
    """
    logger.info("=" * 70)
    logger.info("TRANSFORM: Standardisation des données")
    logger.info("=" * 70)
    
    # Récupérer la liste des fichiers depuis XCom
    ti = context['ti']
    file_paths = ti.xcom_pull(task_ids='extract_to_bronze', key='bronze_files')
    
    if not file_paths:
        raise ValueError("Aucun fichier à traiter")
    
    # Standardiser chaque fichier
    all_dfs = []
    for file_path_str in file_paths:
        file_path = Path(file_path_str)
        try:
            df_standardized = standardize_file(file_path)
            all_dfs.append(df_standardized)
        except Exception as e:
            logger.error(f" Erreur traitement {file_path.name}: {e}")
            raise
    
    # Consolider tous les DataFrames
    df_consolidated = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"\nTotal consolidé: {len(df_consolidated)} lignes")
    
    # Sauvegarder en Parquet localement
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    parquet_filename = f"facturation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    parquet_path = DATA_PROCESSED_DIR / parquet_filename
    
    df_consolidated.to_parquet(parquet_path, index=False, engine='pyarrow')
    logger.info(f"Sauvegardé localement: {parquet_path}")
    
    # Upload vers MinIO silver
    s3_client = get_minio_client()
    object_name = f"processed/{datetime.now().strftime('%Y%m%d')}/{parquet_filename}"
    
    s3_client.upload_file(
        str(parquet_path),
        MINIO_BUCKET_SILVER,
        object_name
    )
    logger.info(f"Uploadé vers silver/{object_name}")
    
    # Pousser le chemin vers XCom
    ti.xcom_push(key='silver_parquet', value=str(parquet_path))
    
    logger.info(f"\nTransformation terminée")
    
    return str(parquet_path)


# ============================================================================
# VALIDATE
# ============================================================================

def validate_quality(**context) -> Dict:
    """
    Tâche 3: Valide la qualité des données transformées.
    
    Returns:
        Rapport de qualité
    """
    logger.info("=" * 70)
    logger.info("VALIDATE: Contrôle qualité")
    logger.info("=" * 70)
    
    # Récupérer le fichier Parquet
    ti = context['ti']
    parquet_path = ti.xcom_pull(task_ids='transform_to_silver', key='silver_parquet')
    
    df = pd.read_parquet(parquet_path)
    logger.info(f"Analyse de {len(df)} lignes")
    
    # Contrôles de qualité
    report = {
        'total_rows': len(df),
        'timestamp': datetime.now().isoformat(),
        'checks': {}
    }
    
    # Check 1: Valeurs manquantes sur champs critiques
    critical_fields = ['patient_id', 'montant_facture', 'date_facturation', 'etablissement_id']
    missing_critical = {}
    for field in critical_fields:
        missing_count = df[field].isnull().sum()
        missing_critical[field] = int(missing_count)
        if missing_count > 0:
            logger.warning(f"  {field}: {missing_count} valeurs manquantes")
    
    report['checks']['missing_critical_fields'] = missing_critical
    
    # Check 2: Montants négatifs
    negative_amounts = (df['montant_facture'] < 0).sum()
    report['checks']['negative_amounts'] = int(negative_amounts)
    if negative_amounts > 0:
        logger.warning(f"  {negative_amounts} montants de facture négatifs")
    
    # Check 3: Dates futures
    future_dates = (df['date_facturation'] > pd.Timestamp.now()).sum()
    report['checks']['future_dates'] = int(future_dates)
    if future_dates > 0:
        logger.warning(f"  {future_dates} dates de facturation dans le futur")
    
    # Check 4: Doublons
    duplicates = df.duplicated(subset=['patient_id', 'date_facturation', 'montant_facture']).sum()
    report['checks']['duplicates'] = int(duplicates)
    if duplicates > 0:
        logger.warning(f"  {duplicates} lignes dupliquées")
    
    # Check 5: Cohérence montants
    incoherent_amounts = (df['montant_paye'] > df['montant_facture']).sum()
    report['checks']['incoherent_amounts'] = int(incoherent_amounts)
    if incoherent_amounts > 0:
        logger.warning(f"  {incoherent_amounts} montants payés > montants facturés")
    
    # Check 6: Cohérence dates
    incoherent_dates = (
        (df['date_paiement'].notna()) & 
        (df['date_paiement'] < df['date_facturation'])
    ).sum()
    report['checks']['incoherent_dates'] = int(incoherent_dates)
    if incoherent_dates > 0:
        logger.warning(f"  {incoherent_dates} dates de paiement < dates de facturation")
    
    # Décision: échouer si problèmes critiques
    critical_issues = (
        missing_critical['patient_id'] > 0 or
        missing_critical['montant_facture'] > 0 or
        negative_amounts > 0
    )
    
    if critical_issues:
        logger.error(" Problèmes critiques détectés - échec de la validation")
        raise ValueError("Validation échouée: problèmes critiques détectés")
    
    logger.info("\n Validation réussie")
    logger.info(f"   Total lignes: {report['total_rows']}")
    logger.info(f"   Doublons: {report['checks']['duplicates']}")
    logger.info(f"   Dates incohérentes: {report['checks']['incoherent_dates']}")
    
    return report


# ============================================================================
# LOAD
# ============================================================================

def load_to_staging(**context):
    """
    Tâche 4: Charge les données dans la table staging.facturation.
    """
    logger.info("=" * 70)
    logger.info("LOAD: Chargement dans staging")
    logger.info("=" * 70)
    
    # Récupérer le fichier Parquet
    ti = context['ti']
    parquet_path = ti.xcom_pull(task_ids='transform_to_silver', key='silver_parquet')
    
    df = pd.read_parquet(parquet_path)
    logger.info(f"Chargement de {len(df)} lignes")
    
    # Filtrer les lignes avec date_facturation NULL (contrainte NOT NULL en base)
    df_invalid = df[df['date_facturation'].isnull()]
    if len(df_invalid) > 0:
        logger.warning(f"WARNING - {len(df_invalid)} lignes ignorées (date_facturation NULL)")
        df = df[df['date_facturation'].notnull()]
        logger.info(f"{len(df)} lignes valides à charger")
    
    # Filtrer les dates de paiement incohérentes (contrainte CHECK en base)
    df_incoherent = df[
        (df['date_paiement'].notnull()) & 
        (df['date_paiement'] < df['date_facturation'])
    ]
    if len(df_incoherent) > 0:
        logger.warning(f"WARNING - {len(df_incoherent)} lignes ignorées (date_paiement < date_facturation)")
        df = df[~(
            (df['date_paiement'].notnull()) & 
            (df['date_paiement'] < df['date_facturation'])
        )]
        logger.info(f"{len(df)} lignes valides à charger")
    
    # Connexion PostgreSQL
    engine = get_postgres_engine()
    
    # Charger dans staging
    df.to_sql(
        'facturation',
        con=engine,
        schema='staging',
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    logger.info(f" {len(df)} lignes chargées dans staging.facturation")


def build_dimensions(**context):
    """
    Tâche 5: Construit les tables de dimensions depuis staging.
    """
    logger.info("=" * 70)
    logger.info("BUILD: Construction des dimensions")
    logger.info("=" * 70)
    
    engine = get_postgres_engine()
    
    with engine.begin() as conn:
        # Dimension Patient
        logger.info(" Construction dim_patient...")
        conn.execute(text("""
            INSERT INTO dw.dim_patient (patient_id_naturel, nom, prenom)
            SELECT DISTINCT 
                patient_id,
                patient_nom,
                patient_prenom
            FROM staging.facturation
            ON CONFLICT (patient_id_naturel) DO NOTHING
        """))
        
        # Dimension Établissement
        logger.info(" Construction dim_etablissement...")
        conn.execute(text("""
            INSERT INTO dw.dim_etablissement (etablissement_id_naturel, nom_etablissement)
            SELECT DISTINCT 
                etablissement_id,
                etablissement_nom
            FROM staging.facturation
            ON CONFLICT (etablissement_id_naturel) DO NOTHING
        """))
        
        # Dimension Service
        logger.info(" Construction dim_service...")
        conn.execute(text("""
            INSERT INTO dw.dim_service (service_code, service_nom)
            SELECT DISTINCT 
                service,
                service
            FROM staging.facturation
            WHERE service IS NOT NULL
            ON CONFLICT (service_code) DO NOTHING
        """))
        
        # Dimension Date
        logger.info(" Construction dim_date...")
        conn.execute(text("""
            INSERT INTO dw.dim_date (date_complete, annee, mois, jour, trimestre)
            SELECT DISTINCT 
                date_facturation,
                EXTRACT(YEAR FROM date_facturation),
                EXTRACT(MONTH FROM date_facturation),
                EXTRACT(DAY FROM date_facturation),
                EXTRACT(QUARTER FROM date_facturation)
            FROM staging.facturation
            WHERE date_facturation IS NOT NULL
            ON CONFLICT (date_complete) DO NOTHING
        """))
    
    logger.info(" Dimensions construites")


def build_fact_table(**context):
    """
    Tâche 6: Construit la table de faits depuis staging + dimensions.
    """
    logger.info("=" * 70)
    logger.info("BUILD: Construction de la table de faits")
    logger.info("=" * 70)
    
    engine = get_postgres_engine()
    
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO dw.fact_facturation (
                patient_key, etablissement_key, date_key, service_key,
                montant_facture, montant_paye, montant_impaye, delai_paiement_jours,
                statut_paiement, nombre_lignes, source_file
            )
            SELECT 
                dp.patient_key,
                de.etablissement_key,
                dd.date_key,
                ds.service_key,
                s.montant_facture,
                s.montant_paye,
                s.montant_impaye,
                s.delai_paiement_jours,
                s.statut_paiement,
                s.nombre_lignes,
                s.source_file
            FROM staging.facturation s
            JOIN dw.dim_patient dp ON s.patient_id = dp.patient_id_naturel
            JOIN dw.dim_etablissement de ON s.etablissement_id = de.etablissement_id_naturel
            JOIN dw.dim_date dd ON s.date_facturation = dd.date_complete
            LEFT JOIN dw.dim_service ds ON s.service = ds.service_code
            WHERE NOT EXISTS (
                SELECT 1 FROM dw.fact_facturation f
                WHERE f.patient_key = dp.patient_key
                  AND f.date_key = dd.date_key
                  AND f.montant_facture = s.montant_facture
            )
        """))
        
        logger.info(f" {result.rowcount} lignes insérées dans fact_facturation")


# ============================================================================
# MACHINE LEARNING
# ============================================================================

def detect_anomalies(**context):
    """
    Tâche 7: Détecte les anomalies avec IsolationForest.
    """
    logger.info("=" * 70)
    logger.info("ML: Détection d'anomalies")
    logger.info("=" * 70)
    
    engine = get_postgres_engine()
    
    # Récupérer les données depuis la table de faits
    query = """
        SELECT 
            fact_id,
            montant_impaye,
            delai_paiement_jours,
            nombre_lignes
        FROM dw.fact_facturation
        WHERE montant_impaye > 0
    """
    
    df = pd.read_sql(query, con=engine)
    logger.info(f" Analyse de {len(df)} créances impayées")
    
    if len(df) < 10:
        logger.warning("  Pas assez de données pour la détection d'anomalies")
        return
    
    # Préparer les features
    features = df[['montant_impaye', 'delai_paiement_jours', 'nombre_lignes']].fillna(0)
    
    # Normalisation
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # Modèle IsolationForest
    model = IsolationForest(
        contamination=0.05,  # 5% d'anomalies attendues
        random_state=42,
        n_estimators=100
    )
    
    df['anomaly_score'] = model.fit_predict(features_scaled)
    
    # Compter les anomalies
    n_anomalies = (df['anomaly_score'] == -1).sum()
    logger.info(f" {n_anomalies} anomalies détectées ({n_anomalies/len(df)*100:.1f}%)")
    
    # Mettre à jour la base de données
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                UPDATE dw.fact_facturation
                SET anomaly_score = :score
                WHERE fact_id = :fact_id
            """), {"score": int(row['anomaly_score']), "fact_id": int(row['fact_id'])})
    
    logger.info(" Scores d'anomalie mis à jour")


def create_datamart_relance(**context):
    """
    Tâche 8: Crée le datamart pour la relance des créances.
    """
    logger.info("=" * 70)
    logger.info("DATAMART: Création datamart_relance")
    logger.info("=" * 70)
    
    engine = get_postgres_engine()
    
    with engine.begin() as conn:
        # Supprimer et recréer la vue matérialisée
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS datamart.relance CASCADE"))
        
        conn.execute(text("""
            CREATE MATERIALIZED VIEW datamart.relance AS
            SELECT 
                f.fact_id,
                p.nom AS patient_nom,
                p.prenom AS patient_prenom,
                e.nom_etablissement,
                s.service_nom,
                f.montant_facture,
                f.montant_paye,
                f.montant_impaye,
                CURRENT_DATE - d.date_complete AS delai_jours,
                f.statut_paiement,
                f.anomaly_score,
                CASE 
                    WHEN f.anomaly_score = -1 THEN 'URGENT'
                    WHEN f.montant_impaye > 5000 THEN 'PRIORITAIRE'
                    WHEN CURRENT_DATE - d.date_complete > 60 THEN 'IMPORTANT'
                    ELSE 'STANDARD'
                END AS priorite,
                d.date_complete AS date_facturation
            FROM dw.fact_facturation f
            JOIN dw.dim_patient p ON f.patient_key = p.patient_key
            JOIN dw.dim_etablissement e ON f.etablissement_key = e.etablissement_key
            JOIN dw.dim_date d ON f.date_key = d.date_key
            LEFT JOIN dw.dim_service s ON f.service_key = s.service_key
            WHERE f.montant_impaye > 0
              AND CURRENT_DATE - d.date_complete > 30
            ORDER BY 
                CASE 
                    WHEN f.anomaly_score = -1 THEN 1
                    WHEN f.montant_impaye > 5000 THEN 2
                    ELSE 3
                END,
                f.montant_impaye DESC
        """))
        
        # Compter les créances
        result = conn.execute(text("SELECT COUNT(*) FROM datamart.relance"))
        count = result.scalar()
        
        logger.info(f" Datamart créé avec {count} créances à relancer")
