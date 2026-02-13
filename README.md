# Pipeline ETL Automatisé de Facturation Médicale

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Apache Airflow 2.8.1](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-150458?logo=pandas)](https://pandas.pydata.org/)
[![PostgreSQL 18](https://img.shields.io/badge/PostgreSQL-18-316192?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docs.docker.com/compose/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.5-F7931E?logo=scikitlearn)](https://scikit-learn.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72E49?logo=minio)](https://min.io/)
[![AWS Ready](https://img.shields.io/badge/AWS-Ready-FF9900?logo=amazonaws)](https://aws.amazon.com/)

> **Projet de démonstration** : Standardisation de données hétérogènes + Data Warehouse en étoile + Orchestration Airflow + Détection d'anomalies ML

---

## ⚠️ Disclaimer / Avertissement

**IMPORTANT** : Ce projet est un **exercice académique personnel** réalisé dans le cadre d'un Master 2 Développement, Big Data & Intelligence Artificielle. 

- Le terme **"Teemeo"** utilisé dans ce projet est **fictif** et ne fait référence à aucune entreprise réelle
- Ce projet **n'a aucun lien** avec la société **KIWEE.Care** ni avec ses produits ou services
- Il s'agit d'un **projet d'entraînement** visant à démontrer des compétences techniques en Data Engineering

**Objectif** : Portfolio technique pour démonstration lors d'entretiens d'embauche dans le domaine du Data Engineering.

---

## Table des Matières

- [Vue d'ensemble](#-vue-densemble)
- [Architecture](#-architecture)
- [Technologies utilisées](#-technologies-utilisées)
- [Schéma du Data Warehouse](#-schéma-du-data-warehouse)
- [Workflow du DAG](#-workflow-du-dag)
- [Prérequis](#-prérequis)
- [Installation](#-installation)
- [Utilisation](#-utilisation)
- [Migration vers AWS](#-migration-vers-aws)
- [Structure du projet](#-structure-du-projet)
- [Requêtes SQL utiles](#-requêtes-sql-utiles)

---

## Vue d'ensemble

Ce projet démontre la mise en place d'un **pipeline ETL complet** pour traiter des données de facturation médicale provenant de **5 établissements** utilisant des formats hétérogènes (CSV avec séparateurs différents, Excel, encodages variés, nommage de colonnes différents).

### Problématique résolue

Les établissements de santé produisent des données de facturation dans des formats incompatibles :
- **Établissement A** : CSV avec `;` et format de dates `dd/mm/yyyy`
- **Établissement B** : CSV avec `,` et format de dates `yyyy-mm-dd`
- **Établissement C** : Fichiers tabulés (`\t`) avec dates ISO
- **Établissement D** : Excel avec colonnes en minuscules
- **Établissement E** : CSV avec encodage spécifique

### Solution implémentée

**Pipeline ETL orchestré par Airflow** qui :
1. **Extrait** les fichiers depuis le système de fichiers vers MinIO (Data Lake Bronze)
2. **Standardise** les données hétérogènes en format Parquet unifié (Silver Layer)
3. **Valide** la qualité des données (6 checks : valeurs manquantes, montants négatifs, dates futures, doublons, cohérence montants, cohérence dates)
4. **Charge** dans une zone de staging PostgreSQL avec métadonnées d'audit
5. **Construit** un Data Warehouse en schéma étoile (4 dimensions + 1 table de faits)
6. **Détecte** les anomalies de paiement avec Machine Learning (IsolationForest)
7. **Crée** un datamart de relance clients avec priorisation automatique

---

## Architecture

### Architecture Medallion (Bronze → Silver → Gold)

```
┌──────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (Data Lake)                     │
│  MinIO S3 - Fichiers bruts CSV/Excel hétérogènes (5 établissements) │
└────────────────────────────┬─────────────────────────────────────────┘
                             │ ETL Extract + Transform
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (Data Lake)                     │
│          MinIO S3 - Fichiers Parquet standardisés + validés          │
└────────────────────────────┬─────────────────────────────────────────┘
                             │ ETL Load + Build Dimensions
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Data Warehouse)                       │
│                          PostgreSQL 18                               │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────────┐  │
│  │ staging schema │  │    dw schema     │  │ datamart schema    │  │
│  │  - facturation │  │ - dim_patient    │  │ - relance (MV)     │  │
│  │                │  │ - dim_etabl.     │  │                    │  │
│  │                │  │ - dim_date       │  │                    │  │
│  │                │  │ - dim_service    │  │                    │  │
│  │                │  │ - fact_factur.   │  │                    │  │
│  └────────────────┘  └──────────────────┘  └────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### Schéma en étoile (Star Schema)

```
                             ┌─────────────────┐
                             │  dim_patient    │
                             │─────────────────│
                             │ patient_key (PK)│
                      ┌──────│ patient_id      │
                      │      │ nom             │
                      │      │ prenom          │
                      │      │ age             │
                      │      └─────────────────┘
                      │
┌─────────────────┐   │      ┌─────────────────────────────┐
│ dim_etabl.      │   │      │   fact_facturation          │
│─────────────────│   │      │─────────────────────────────│
│ etabl_key (PK)  │◄──┼──────│ facture_id (PK)             │
│ etabl_id        │   │      │ patient_key (FK)            │
│ nom_etabl.      │   └─────►│ etablissement_key (FK)      │
│ type_etabl.     │      ┌───│ date_key (FK)               │
│ ville           │      │   │ service_key (FK)            │
└─────────────────┘      │   │ montant_facture             │
                         │   │ montant_paye                │
         ┌───────────────┘   │ montant_impaye              │
         │                   │ delai_paiement_jours        │
         │                   │ anomaly_score               │
         │                   └─────────────────────────────┘
         │                              ▲
         │      ┌───────────────┐       │
         │      │  dim_service  │       │
         │      │───────────────│       │
         └─────►│ service_key   │───────┘
                │ code_service  │
                │ nom_service   │
                │ categorie     │
                └───────────────┘
                        │
                        ▼
                ┌───────────────┐
                │   dim_date    │
                │───────────────│
                │ date_key (PK) │
                │ date_complete │
                │ annee         │
                │ mois          │
                │ jour          │
                │ trimestre     │
                │ nom_mois      │
                │ jour_semaine  │
                └───────────────┘
```

---

## Technologies utilisées

| Composant | Version | Rôle |
|-----------|---------|------|
| **Python** | 3.11 | Langage principal pour le traitement ETL |
| **Apache Airflow** | 2.8.1 | Orchestration du pipeline (LocalExecutor) |
| **PostgreSQL** | 18 | Data Warehouse relationnel (OLAP) |
| **MinIO** | Latest | Data Lake S3-compatible (Bronze/Silver) |
| **Pandas** | 2.0+ | Manipulation et transformation des données |
| **SQLAlchemy** | 2.0+ | ORM et connexion base de données |
| **boto3** | 1.34+ | Client S3 pour MinIO |
| **scikit-learn** | 1.5+ | Détection d'anomalies (IsolationForest) |
| **openpyxl** | 3.1+ | Lecture de fichiers Excel |
| **Docker Compose** | Latest | Orchestration des conteneurs |
| **Adminer** | Latest | Interface web pour PostgreSQL |

### Bibliothèques Python clés

```python
# Traitement de données
pandas==2.2.2
numpy==1.26.4
openpyxl==3.1.2

# Connexion base de données
psycopg2-binary==2.9.9
SQLAlchemy==2.0.30

# Stockage S3
boto3==1.34.110

# Machine Learning
scikit-learn==1.5.0

# Airflow
apache-airflow==2.8.1
```

---

## Schéma du Data Warehouse

### Schémas PostgreSQL

Le Data Warehouse est organisé en 3 schémas :

#### 1. **staging** - Zone de validation
- `staging.facturation` : Table temporaire avec métadonnées d'audit (`source_file`, `load_timestamp`)

#### 2. **dw** - Data Warehouse principal (schéma en étoile)

**Tables de dimensions** :
- `dw.dim_patient` : Patients (clé de substitution `patient_key`)
- `dw.dim_etablissement` : Établissements de santé (clé de substitution `etablissement_key`)
- `dw.dim_date` : Dimension temporelle avec attributs calculés (année, mois, trimestre, jour_semaine)
- `dw.dim_service` : Services médicaux (consultations, urgences, chirurgie, etc.)

**Table de faits** :
- `dw.fact_facturation` : Faits de facturation avec métriques additives
  - `montant_facture` (NUMERIC(10,2))
  - `montant_paye` (NUMERIC(10,2))
  - `montant_impaye` (NUMERIC(10,2) - calculé avec CHECK constraint)
  - `delai_paiement_jours` (INTEGER)
  - `anomaly_score` (FLOAT - score ML entre -1 et 1)

#### 3. **datamart** - Vues métier
- `datamart.relance` : Vue matérialisée pour la gestion des relances clients
  - Priorités automatiques : URGENT (anomalies), PRIORITAIRE (>5000€), STANDARD
  - Agrégations par patient : `total_impaye`, `nb_factures_impayees`

### Contraintes de qualité

```sql
-- Contrainte sur montants
CHECK (montant_impaye = montant_facture - montant_paye)

-- Contrainte sur dates
CHECK (date_paiement IS NULL OR date_paiement >= date_facture)

-- Index pour performances
CREATE INDEX idx_fact_patient ON dw.fact_facturation(patient_key);
CREATE INDEX idx_fact_etablissement ON dw.fact_facturation(etablissement_key);
CREATE INDEX idx_fact_date ON dw.fact_facturation(date_key);
CREATE INDEX idx_fact_impaye ON dw.fact_facturation(montant_impaye) WHERE montant_impaye > 0;
```

---

## Workflow du DAG

Le DAG `teemeo_billing_pipeline` orchestre 9 tâches exécutées quotidiennement :

```
start_pipeline
      │
      ▼
extract_to_bronze ───→ Upload fichiers CSV/Excel vers MinIO bucket 'bronze'
      │
      ▼
transform_to_silver ─→ Standardisation + sauvegarde Parquet dans bucket 'silver'
      │
      ▼
validate_quality ────→ 6 checks qualité (lève ValueError si échec critique)
      │
      ▼
load_to_staging ─────→ Insertion dans staging.facturation avec métadonnées
      │
      ▼
build_dimensions ────→ Remplissage des 4 tables de dimensions (idempotent)
      │
      ▼
build_fact_table ────→ Insertion dans fact_facturation avec clés étrangères
      │
      ▼
detect_anomalies ────→ IsolationForest (contamination=5%) + mise à jour anomaly_score
      │
      ▼
create_datamart ─────→ Rafraîchissement de la vue matérialisée datamart.relance
      │
      ▼
pipeline_summary ────→ Log du résumé d'exécution
```

### Configuration du DAG

```python
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'teemeo_billing_pipeline',
    default_args=default_args,
    description='Pipeline ETL facturation médicale avec ML',
    schedule_interval='@daily',  # Exécution quotidienne
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'billing', 'healthcare', 'ml']
)
```

---

## Prérequis

- **Docker** et **Docker Compose** installés
- **Python 3.11+** (pour développement local)
- **8 GB RAM** minimum recommandés
- **Ports disponibles** : 8080 (Airflow), 5432 (PostgreSQL), 9080 (Adminer), 9000 (MinIO), 9001 (MinIO Console)

---

## Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/votre-username/teemeo-billing-etl-airflow.git
cd teemeo-billing-etl-airflow
```

### 2. Configuration de l'environnement

Le fichier `.env` est déjà configuré avec les paramètres par défaut :

```bash
# PostgreSQL
POSTGRES_USER=teemeo
POSTGRES_PASSWORD=teemeo123
POSTGRES_DB=teemeo_dw

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__FERNET_KEY=<votre_clé_générée>
AIRFLOW__CORE__LOAD_EXAMPLES=False

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

### 3. Générer les données de test

```bash
# Créer le répertoire data si nécessaire
mkdir -p data

# Générer les fichiers CSV/Excel hétérogènes
docker compose run --rm airflow-cli python /opt/airflow/scripts/generate_sample_data.py
```

Cela crée 6 fichiers dans `data/` :
- `etablissement_A.csv` (séparateur `;`, dates FR)
- `etablissement_B.csv` (séparateur `,`, dates US)
- `etablissement_C.csv` (séparateur `\t`, dates ISO)
- `etablissement_D.csv` (séparateur `;`, colonnes minuscules)
- `etablissement_E.xlsx` (Excel)

### 4. Démarrer l'environnement

```bash
# Démarrer tous les conteneurs
docker compose up -d

# Vérifier que tous les services sont UP
docker compose ps
```

### 5. Accéder aux interfaces

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Airflow Web UI** | http://localhost:8080 | `airflow` / `airflow` |
| **Adminer (PostgreSQL)** | http://localhost:9080 | Serveur: `postgres`, User: `teemeo`, Pass: `teemeo123`, DB: `teemeo_dw` |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |

---

## Utilisation

### Exécuter le pipeline ETL

1. **Ouvrir Airflow** : http://localhost:8080
2. **Activer le DAG** `teemeo_billing_pipeline` (toggle sur ON)
3. **Déclencher manuellement** : Cliquer sur le bouton Play
4. **Suivre l'exécution** : Cliquer sur le DAG → Graph View

### Vérifier les résultats

#### Via Adminer (http://localhost:9080)

```sql
-- Nombre de factures par établissement
SELECT 
    e.nom_etablissement,
    COUNT(*) as nb_factures,
    SUM(f.montant_facture) as total_facture,
    SUM(f.montant_impaye) as total_impaye
FROM dw.fact_facturation f
JOIN dw.dim_etablissement e ON f.etablissement_key = e.etablissement_key
GROUP BY e.nom_etablissement
ORDER BY total_impaye DESC;

-- Clients avec anomalies de paiement
SELECT 
    p.nom,
    p.prenom,
    f.montant_impaye,
    f.anomaly_score,
    f.date_facture,
    e.nom_etablissement
FROM dw.fact_facturation f
JOIN dw.dim_patient p ON f.patient_key = p.patient_key
JOIN dw.dim_etablissement e ON f.etablissement_key = e.etablissement_key
WHERE f.anomaly_score < -0.1  -- Anomalies détectées
ORDER BY f.anomaly_score ASC
LIMIT 20;

-- Datamart de relance (vue matérialisée)
SELECT 
    priorite,
    COUNT(*) as nb_patients,
    SUM(total_impaye) as montant_total
FROM datamart.relance
GROUP BY priorite
ORDER BY 
    CASE priorite
        WHEN 'URGENT' THEN 1
        WHEN 'PRIORITAIRE' THEN 2
        ELSE 3
    END;
```

#### Via ligne de commande

```bash
# Se connecter à PostgreSQL
docker compose exec postgres psql -U teemeo -d teemeo_dw

# Compter les enregistrements
teemeo_dw=# SELECT COUNT(*) FROM dw.fact_facturation;
teemeo_dw=# SELECT COUNT(*) FROM dw.dim_patient;
```

### Logs Airflow

```bash
# Logs du scheduler
docker compose logs -f airflow-scheduler

# Logs du webserver
docker compose logs -f airflow-webserver

# Logs d'une tâche spécifique (depuis l'UI Graph View → clic sur tâche → Logs)
```

---

## Migration vers AWS

Ce projet est conçu pour être facilement migré vers AWS :

### Architecture AWS cible

```
┌─────────────────────────────────────────────────────────────┐
│                        AMAZON S3                            │
│  Bronze Bucket: s3://teemeo-bronze/    (CSV/Excel bruts)   │
│  Silver Bucket: s3://teemeo-silver/    (Parquet validés)   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              AMAZON MWAA (Managed Airflow)                  │
│  - DAGs déployés depuis S3                                  │
│  - Scalabilité automatique des workers                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    AMAZON RDS PostgreSQL                    │
│  - Multi-AZ pour haute disponibilité                        │
│  - Snapshots automatiques                                   │
│  - Read Replicas pour requêtes analytiques                  │
└─────────────────────────────────────────────────────────────┘
```

### Modifications requises

#### 1. Remplacer MinIO par S3

```python
# Dans etl_tasks.py - Remplacer boto3 client MinIO
s3_client = boto3.client(
    's3',
    region_name='eu-west-1',  # Votre région AWS
    # Les credentials sont gérés par IAM Roles dans MWAA
)
```

#### 2. Mettre à jour les connexions Airflow

```bash
# Via Airflow UI → Admin → Connections
# OU via AWS Secrets Manager (recommandé)

# Connection PostgreSQL
aws_db_connection = {
    'conn_id': 'postgres_teemeo',
    'conn_type': 'postgres',
    'host': 'teemeo-dw.xxxxx.eu-west-1.rds.amazonaws.com',
    'schema': 'teemeo_dw',
    'login': 'teemeo',
    'password': '${POSTGRES_PASSWORD}',  # Depuis Secrets Manager
    'port': 5432
}
```

#### 3. Déployer les DAGs sur MWAA

```bash
# Créer un bucket S3 pour les DAGs
aws s3 mb s3://teemeo-mwaa-dags

# Copier les DAGs et scripts
aws s3 sync dags/ s3://teemeo-mwaa-dags/dags/
aws s3 sync scripts/ s3://teemeo-mwaa-dags/scripts/

# Copier requirements.txt (MWAA l'installe automatiquement)
aws s3 cp requirements.txt s3://teemeo-mwaa-dags/
```

#### 4. Infrastructure as Code (Terraform)

```hcl
# Exemple de configuration Terraform
module "mwaa" {
  source = "terraform-aws-modules/mwaa/aws"
  
  name               = "teemeo-billing-etl"
  airflow_version    = "2.8.1"
  environment_class  = "mw1.small"
  
  source_bucket_arn  = aws_s3_bucket.mwaa_dags.arn
  dag_s3_path        = "dags"
  requirements_s3_path = "requirements.txt"
  
  max_workers        = 5
  min_workers        = 1
}
```

### Coûts estimés AWS (région eu-west-1)

| Service | Configuration | Coût mensuel (USD) |
|---------|---------------|-------------------|
| MWAA | mw1.small (1 worker) | ~$200 |
| RDS PostgreSQL | db.t3.medium | ~$70 |
| S3 | 50 GB stockage | ~$1.15 |
| Data Transfer | 10 GB sortant | ~$0.90 |
| **TOTAL** | | **~$272/mois** |

---

## Structure du projet

```
teemeo-billing-etl-airflow/
│
├── dags/
│   └── teemeo_billing_pipeline.py      # DAG Airflow principal
│
├── scripts/
│   ├── etl_tasks.py                    # Fonctions ETL (extract, transform, load, ML)
│   └── generate_sample_data.py         # Générateur de données hétérogènes
│
├── vol/
│   └── postgresql/
│       ├── 01-init-databases.sh        # Script d'initialisation PostgreSQL
│       └── create_schemas_and_tables.sql  # Schéma DW (staging, dw, datamart)
│
├── data/                                # Fichiers CSV/Excel sources
│   ├── etablissement_A.csv
│   ├── etablissement_B.csv
│   ├── etablissement_C.csv
│   ├── etablissement_D.csv
│   └── etablissement_E.xlsx
│
├── logs/                                # Logs Airflow (généré automatiquement)
│
├── docker-compose.yml                  # Orchestration des conteneurs
├── requirements.txt                    # Dépendances Python
├── .env                                # Variables d'environnement
├── .gitignore                          # Fichiers à ignorer par Git
├── LICENSE                             # Licence MIT
└── README.md                           # Documentation complète
```

---

## Requêtes SQL utiles

### Analyse de la qualité des données

```sql
-- Taux de paiement par service médical
SELECT 
    s.nom_service,
    COUNT(*) as nb_factures,
    SUM(f.montant_facture) as total_facture,
    SUM(f.montant_paye) as total_paye,
    ROUND(100.0 * SUM(f.montant_paye) / NULLIF(SUM(f.montant_facture), 0), 2) as taux_paiement_pct
FROM dw.fact_facturation f
JOIN dw.dim_service s ON f.service_key = s.service_key
GROUP BY s.nom_service
ORDER BY taux_paiement_pct;

-- Top 10 des retards de paiement
SELECT 
    p.nom || ' ' || p.prenom as patient,
    e.nom_etablissement,
    f.date_facture,
    f.date_paiement,
    f.delai_paiement_jours,
    f.montant_impaye
FROM dw.fact_facturation f
JOIN dw.dim_patient p ON f.patient_key = p.patient_key
JOIN dw.dim_etablissement e ON f.etablissement_key = e.etablissement_key
WHERE f.delai_paiement_jours IS NOT NULL
ORDER BY f.delai_paiement_jours DESC
LIMIT 10;

-- Analyse temporelle (tendances mensuelles)
SELECT 
    d.annee,
    d.nom_mois,
    COUNT(*) as nb_factures,
    SUM(f.montant_facture) as ca_total,
    AVG(f.montant_impaye) as montant_impaye_moyen
FROM dw.fact_facturation f
JOIN dw.dim_date d ON f.date_key = d.date_key
GROUP BY d.annee, d.mois, d.nom_mois
ORDER BY d.annee, d.mois;
```

### Requêtes pour le Machine Learning

```sql
-- Distribution des scores d'anomalie
SELECT 
    CASE 
        WHEN anomaly_score < -0.2 THEN 'Anomalie forte'
        WHEN anomaly_score < -0.1 THEN 'Anomalie modérée'
        WHEN anomaly_score < 0 THEN 'Anomalie faible'
        ELSE 'Normal'
    END as categorie_anomalie,
    COUNT(*) as nb_factures,
    AVG(montant_impaye) as montant_impaye_moyen
FROM dw.fact_facturation
WHERE montant_impaye > 0
GROUP BY 
    CASE 
        WHEN anomaly_score < -0.2 THEN 'Anomalie forte'
        WHEN anomaly_score < -0.1 THEN 'Anomalie modérée'
        WHEN anomaly_score < 0 THEN 'Anomalie faible'
        ELSE 'Normal'
    END
ORDER BY montant_impaye_moyen DESC;

-- Features utilisées pour la détection d'anomalies
SELECT 
    montant_impaye,
    delai_paiement_jours,
    montant_facture,
    anomaly_score,
    CASE WHEN anomaly_score < -0.1 THEN 'ANOMALIE' ELSE 'NORMAL' END as statut
FROM dw.fact_facturation
WHERE montant_impaye > 0
ORDER BY anomaly_score ASC
LIMIT 50;
```

---

## Contribution

Les contributions sont les bienvenues ! Pour contribuer :

1. **Forkez** le dépôt
2. **Créez une branche** : `git checkout -b feature/amelioration-etl`
3. **Committez** : `git commit -m 'Ajout validation avancée'`
4. **Pushez** : `git push origin feature/amelioration-etl`
5. **Ouvrez une Pull Request**

---

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

---

## Contact

**Projet de démonstration** pour entretiens techniques dans le domaine de l'ingénierie des données.

Pour toute question : [darilgylls@gmail.com](darilgylls@gmail.com)

---

## Compétences démontrées

**ETL** : Extraction, transformation, chargement de données hétérogènes  
**Orchestration** : Airflow DAGs avec gestion d'erreurs et retry logic  
**Data Warehouse** : Schéma en étoile (Kimball), clés de substitution  
**Data Lake** : Architecture Medallion (Bronze/Silver/Gold)  
**SQL avancé** : CTE, window functions, contraintes CHECK  
**Python** : Pandas, NumPy, SQLAlchemy, boto3  
**Machine Learning** : scikit-learn IsolationForest pour détection d'anomalies  
**Docker** : Environnement multi-conteneurs avec Compose  
**Cloud Ready** : Architecture migratable vers AWS (S3, MWAA, RDS)  
**Data Quality** : 6 checks de validation automatisés  
**Documentation** : README complet avec diagrammes et exemples

---

**N'oubliez pas de mettre une étoile si ce projet vous a été utile !**
