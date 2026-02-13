#!/bin/bash
set -e

# Script d'initialisation PostgreSQL exécuté au démarrage du container
# Crée les bases de données et utilisateurs nécessaires

echo " Initialisation des bases de données PostgreSQL..."

# Créer les utilisateurs et bases de données si elles n'existent pas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    
    -- Base de données Airflow
    CREATE DATABASE airflow;
    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    
    -- Base de données Data Warehouse
    CREATE DATABASE teemeo_dw;
    CREATE USER teemeo_dw WITH PASSWORD 'teemeo_dw';
    GRANT ALL PRIVILEGES ON DATABASE teemeo_dw TO teemeo_dw;
    
    -- Permissions supplémentaires
    ALTER USER airflow CREATEDB;
    ALTER USER teemeo_dw CREATEDB;

EOSQL

# Donner les permissions sur le schéma public pour airflow
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "airflow" <<-EOSQL
    GRANT ALL ON SCHEMA public TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;
EOSQL

# Donner les permissions sur le schéma public pour teemeo_dw
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "teemeo_dw" <<-EOSQL
    GRANT ALL ON SCHEMA public TO teemeo_dw;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO teemeo_dw;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO teemeo_dw;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO teemeo_dw;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO teemeo_dw;
EOSQL

echo " Bases de données créées:"
echo "   - airflow (user: airflow)"
echo "   - teemeo_dw (user: teemeo_dw)"

# Exécuter le script de création des schémas et tables
echo ""
echo "Création des schémas et tables du Data Warehouse..."

# Attendre que la base soit prête
sleep 2

# Exécuter le script SQL de création des tables
psql -v ON_ERROR_STOP=1 --username "teemeo_dw" --dbname "teemeo_dw" < /docker-entrypoint-initdb.d/create_schemas_and_tables.sql

echo ""
echo "Initialisation PostgreSQL terminée !"
