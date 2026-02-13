-- ============================================================================
-- Script de création des schémas et tables pour le Data Warehouse
-- Projet: Teemeo Billing ETL Pipeline
-- Base de données: PostgreSQL
-- ============================================================================

-- ============================================================================
-- CRÉATION DES SCHÉMAS
-- ============================================================================

-- Schéma STAGING: zone tampon pour les données brutes nettoyées
CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS 'Zone de staging pour les données brutes standardisées avant intégration au DW';

-- Schéma DW: schéma en étoile (dimensions + faits)
CREATE SCHEMA IF NOT EXISTS dw;
COMMENT ON SCHEMA dw IS 'Data Warehouse - Schéma en étoile avec dimensions et table de faits';

-- Schéma DATAMART: vues métier dénormalisées
CREATE SCHEMA IF NOT EXISTS datamart;
COMMENT ON SCHEMA datamart IS 'Datamarts métier pour analyses spécifiques';


-- ============================================================================
-- STAGING: Tables de travail
-- ============================================================================

-- Suppression de la table staging si elle existe (pour rejouabilité)
DROP TABLE IF EXISTS staging.facturation CASCADE;

-- Table de staging: données standardisées mais pas encore structurées en étoile
CREATE TABLE staging.facturation (
    staging_id SERIAL PRIMARY KEY,
    
    -- Identifiants patient
    patient_id VARCHAR(50) NOT NULL,
    patient_nom VARCHAR(100),
    patient_prenom VARCHAR(100),
    
    -- Identifiants établissement
    etablissement_id VARCHAR(50) NOT NULL,
    etablissement_nom VARCHAR(200),
    
    -- Service médical
    service VARCHAR(100),
    
    -- Dates
    date_facturation DATE NOT NULL,
    date_paiement DATE,
    
    -- Montants
    montant_facture NUMERIC(12, 2) NOT NULL,
    montant_paye NUMERIC(12, 2) DEFAULT 0,
    montant_impaye NUMERIC(12, 2),
    
    -- Statut et détails
    statut_paiement VARCHAR(50),
    nombre_lignes INTEGER DEFAULT 1,
    delai_paiement_jours INTEGER,
    
    -- Métadonnées (traçabilité)
    source_file VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contraintes
    CONSTRAINT chk_montant_facture_positive CHECK (montant_facture >= 0),
    CONSTRAINT chk_montant_paye_positive CHECK (montant_paye >= 0),
    CONSTRAINT chk_dates_coherentes CHECK (
        date_paiement IS NULL OR date_paiement >= date_facturation
    )
);

-- Index pour performances
CREATE INDEX idx_staging_patient ON staging.facturation(patient_id);
CREATE INDEX idx_staging_etablissement ON staging.facturation(etablissement_id);
CREATE INDEX idx_staging_date_facturation ON staging.facturation(date_facturation);

COMMENT ON TABLE staging.facturation IS 'Table de staging contenant les données standardisées avant intégration au DW';


-- ============================================================================
-- DW: TABLES DE DIMENSIONS
-- ============================================================================

-- Dimension PATIENT
DROP TABLE IF EXISTS dw.dim_patient CASCADE;

CREATE TABLE dw.dim_patient (
    patient_key SERIAL PRIMARY KEY,                  -- Surrogate key (clé technique)
    patient_id_naturel VARCHAR(50) UNIQUE NOT NULL,  -- Business key (clé métier)
    nom VARCHAR(100),
    prenom VARCHAR(100),
    
    -- Pour SCD Type 2 (optionnel - historisation)
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index
CREATE INDEX idx_dim_patient_natural_key ON dw.dim_patient(patient_id_naturel);
CREATE INDEX idx_dim_patient_is_current ON dw.dim_patient(is_current) WHERE is_current = TRUE;

COMMENT ON TABLE dw.dim_patient IS 'Dimension Patient avec clé de substitution';
COMMENT ON COLUMN dw.dim_patient.patient_key IS 'Clé technique auto-incrémentée (surrogate key)';
COMMENT ON COLUMN dw.dim_patient.patient_id_naturel IS 'Identifiant métier du patient (business key)';


-- Dimension ÉTABLISSEMENT
DROP TABLE IF EXISTS dw.dim_etablissement CASCADE;

CREATE TABLE dw.dim_etablissement (
    etablissement_key SERIAL PRIMARY KEY,
    etablissement_id_naturel VARCHAR(50) UNIQUE NOT NULL,
    nom_etablissement VARCHAR(200),
    
    -- Informations complémentaires (à enrichir)
    ville VARCHAR(100),
    region VARCHAR(100),
    type_etablissement VARCHAR(50),  -- Clinique, Hôpital, Centre...
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_etablissement_natural_key ON dw.dim_etablissement(etablissement_id_naturel);

COMMENT ON TABLE dw.dim_etablissement IS 'Dimension Établissement de santé';


-- Dimension DATE
DROP TABLE IF EXISTS dw.dim_date CASCADE;

CREATE TABLE dw.dim_date (
    date_key SERIAL PRIMARY KEY,
    date_complete DATE UNIQUE NOT NULL,
    
    -- Décomposition temporelle
    annee INTEGER NOT NULL,
    mois INTEGER NOT NULL,
    jour INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    semaine INTEGER,
    jour_semaine INTEGER,
    nom_jour VARCHAR(20),
    nom_mois VARCHAR(20),
    
    -- Indicateurs
    est_weekend BOOLEAN,
    est_jour_ferie BOOLEAN DEFAULT FALSE,
    
    -- Périodes fiscales
    annee_fiscale INTEGER,
    trimestre_fiscal INTEGER,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_complete ON dw.dim_date(date_complete);
CREATE INDEX idx_dim_date_annee_mois ON dw.dim_date(annee, mois);

COMMENT ON TABLE dw.dim_date IS 'Dimension Date avec décomposition temporelle complète';


-- Dimension SERVICE
DROP TABLE IF EXISTS dw.dim_service CASCADE;

CREATE TABLE dw.dim_service (
    service_key SERIAL PRIMARY KEY,
    service_code VARCHAR(100) UNIQUE NOT NULL,
    service_nom VARCHAR(200),
    
    -- Classification
    categorie VARCHAR(100),  -- Médical, Chirurgical, Technique...
    departement VARCHAR(100),
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_service_code ON dw.dim_service(service_code);

COMMENT ON TABLE dw.dim_service IS 'Dimension Service médical';


-- ============================================================================
-- DW: TABLE DE FAITS
-- ============================================================================

DROP TABLE IF EXISTS dw.fact_facturation CASCADE;

CREATE TABLE dw.fact_facturation (
    fact_id SERIAL PRIMARY KEY,
    
    -- Clés étrangères vers les dimensions (Foreign Keys)
    patient_key INTEGER NOT NULL,
    etablissement_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    service_key INTEGER,  -- Nullable si service inconnu
    
    -- Mesures (UNIQUEMENT des valeurs numériques)
    montant_facture NUMERIC(12, 2) NOT NULL,
    montant_paye NUMERIC(12, 2) NOT NULL,
    montant_impaye NUMERIC(12, 2) NOT NULL,
    delai_paiement_jours INTEGER,
    nombre_lignes INTEGER DEFAULT 1,
    
    -- Attributs dégénérés (colonnes descriptives qui ne méritent pas une dimension)
    statut_paiement VARCHAR(50),
    
    -- Score d'anomalie (ML)
    anomaly_score INTEGER,  -- -1 = anomalie, 1 = normal
    
    -- Métadonnées
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contraintes de clés étrangères
    CONSTRAINT fk_fact_patient FOREIGN KEY (patient_key) 
        REFERENCES dw.dim_patient(patient_key),
    CONSTRAINT fk_fact_etablissement FOREIGN KEY (etablissement_key) 
        REFERENCES dw.dim_etablissement(etablissement_key),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_key) 
        REFERENCES dw.dim_date(date_key),
    CONSTRAINT fk_fact_service FOREIGN KEY (service_key) 
        REFERENCES dw.dim_service(service_key),
    
    -- Contraintes métier
    CONSTRAINT chk_fact_montant_facture CHECK (montant_facture >= 0),
    CONSTRAINT chk_fact_montant_paye CHECK (montant_paye >= 0),
    CONSTRAINT chk_fact_coherence_montants CHECK (
        montant_impaye = montant_facture - montant_paye
    )
);

-- Index pour performances (requêtes analytiques)
CREATE INDEX idx_fact_patient_key ON dw.fact_facturation(patient_key);
CREATE INDEX idx_fact_etablissement_key ON dw.fact_facturation(etablissement_key);
CREATE INDEX idx_fact_date_key ON dw.fact_facturation(date_key);
CREATE INDEX idx_fact_service_key ON dw.fact_facturation(service_key);

-- Index composite pour requêtes fréquentes
CREATE INDEX idx_fact_etablissement_date ON dw.fact_facturation(etablissement_key, date_key);
CREATE INDEX idx_fact_anomalies ON dw.fact_facturation(anomaly_score) WHERE anomaly_score = -1;
CREATE INDEX idx_fact_impayes ON dw.fact_facturation(montant_impaye) WHERE montant_impaye > 0;

COMMENT ON TABLE dw.fact_facturation IS 'Table de faits centrale - Une ligne = une facture';
COMMENT ON COLUMN dw.fact_facturation.fact_id IS 'Identifiant unique de la facture (clé primaire)';
COMMENT ON COLUMN dw.fact_facturation.montant_impaye IS 'Montant restant à payer (calculé)';
COMMENT ON COLUMN dw.fact_facturation.anomaly_score IS 'Score ML: -1=anomalie, 1=normal';


-- ============================================================================
-- FONCTIONS UTILITAIRES
-- ============================================================================

-- Fonction pour mettre à jour le timestamp updated_at automatiquement
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers pour auto-update des timestamps
CREATE TRIGGER trg_dim_patient_updated_at
    BEFORE UPDATE ON dw.dim_patient
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_dim_etablissement_updated_at
    BEFORE UPDATE ON dw.dim_etablissement
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_dim_service_updated_at
    BEFORE UPDATE ON dw.dim_service
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ============================================================================
-- VUES UTILES POUR ANALYSES
-- ============================================================================

-- Vue: Vue dénormalisée de la table de faits avec toutes les dimensions
CREATE OR REPLACE VIEW dw.v_facturation_complete AS
SELECT 
    f.fact_id,
    f.created_at AS date_creation_enregistrement,
    
    -- Dimension Patient
    p.patient_id_naturel,
    p.nom AS patient_nom,
    p.prenom AS patient_prenom,
    
    -- Dimension Établissement
    e.etablissement_id_naturel,
    e.nom_etablissement,
    e.ville AS etablissement_ville,
    e.type_etablissement,
    
    -- Dimension Date
    d.date_complete AS date_facturation,
    d.annee,
    d.mois,
    d.trimestre,
    d.nom_mois,
    
    -- Dimension Service
    s.service_nom,
    s.categorie AS service_categorie,
    
    -- Mesures
    f.montant_facture,
    f.montant_paye,
    f.montant_impaye,
    f.delai_paiement_jours,
    f.nombre_lignes,
    f.statut_paiement,
    f.anomaly_score,
    
    -- Métadonnées
    f.source_file
FROM dw.fact_facturation f
JOIN dw.dim_patient p ON f.patient_key = p.patient_key
JOIN dw.dim_etablissement e ON f.etablissement_key = e.etablissement_key
JOIN dw.dim_date d ON f.date_key = d.date_key
LEFT JOIN dw.dim_service s ON f.service_key = s.service_key;

COMMENT ON VIEW dw.v_facturation_complete IS 'Vue dénormalisée de la table de faits avec toutes les dimensions jointes';


-- ============================================================================
-- STATISTIQUES ET MONITORING
-- ============================================================================

-- Vue: Statistiques par établissement
CREATE OR REPLACE VIEW dw.v_stats_etablissement AS
SELECT 
    e.nom_etablissement,
    COUNT(*) AS nombre_factures,
    SUM(f.montant_facture) AS total_facture,
    SUM(f.montant_paye) AS total_paye,
    SUM(f.montant_impaye) AS total_impaye,
    ROUND(AVG(f.montant_facture), 2) AS montant_moyen_facture,
    ROUND(AVG(f.delai_paiement_jours), 1) AS delai_moyen_paiement_jours,
    SUM(CASE WHEN f.anomaly_score = -1 THEN 1 ELSE 0 END) AS nombre_anomalies
FROM dw.fact_facturation f
JOIN dw.dim_etablissement e ON f.etablissement_key = e.etablissement_key
GROUP BY e.nom_etablissement
ORDER BY total_facture DESC;

COMMENT ON VIEW dw.v_stats_etablissement IS 'Statistiques agrégées par établissement';


-- Vue: Évolution mensuelle du chiffre d'affaires
CREATE OR REPLACE VIEW dw.v_ca_mensuel AS
SELECT 
    d.annee,
    d.mois,
    d.nom_mois,
    COUNT(*) AS nombre_factures,
    SUM(f.montant_facture) AS ca_facture,
    SUM(f.montant_paye) AS ca_encaisse,
    SUM(f.montant_impaye) AS ca_impaye,
    ROUND(
        100.0 * SUM(f.montant_paye) / NULLIF(SUM(f.montant_facture), 0), 
        2
    ) AS taux_recouvrement_pct
FROM dw.fact_facturation f
JOIN dw.dim_date d ON f.date_key = d.date_key
GROUP BY d.annee, d.mois, d.nom_mois
ORDER BY d.annee DESC, d.mois DESC;

COMMENT ON VIEW dw.v_ca_mensuel IS 'Évolution mensuelle du chiffre d''affaires et taux de recouvrement';


-- ============================================================================
-- PERMISSIONS (à adapter selon les rôles)
-- ============================================================================

-- Permissions pour le user teemeo_dw (création via script init)
GRANT ALL PRIVILEGES ON SCHEMA staging TO teemeo_dw;
GRANT ALL PRIVILEGES ON SCHEMA dw TO teemeo_dw;
GRANT ALL PRIVILEGES ON SCHEMA datamart TO teemeo_dw;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO teemeo_dw;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO teemeo_dw;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA datamart TO teemeo_dw;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO teemeo_dw;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dw TO teemeo_dw;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA datamart TO teemeo_dw;

-- Permissions futures tables (exécuté en tant que teemeo_dw)
ALTER DEFAULT PRIVILEGES IN SCHEMA staging 
    GRANT ALL PRIVILEGES ON TABLES TO teemeo_dw;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw 
    GRANT ALL PRIVILEGES ON TABLES TO teemeo_dw;
ALTER DEFAULT PRIVILEGES IN SCHEMA datamart 
    GRANT ALL PRIVILEGES ON TABLES TO teemeo_dw;


-- ============================================================================
-- SCRIPT TERMINÉ
-- ============================================================================

-- Afficher un résumé
DO $$
DECLARE 
    v_count INTEGER;
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Schémas et tables créés avec succès !';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Schémas créés:';
    RAISE NOTICE '  - staging   : Tables de travail';
    RAISE NOTICE '  - dw        : Data Warehouse (schéma en étoile)';
    RAISE NOTICE '  - datamart  : Datamarts métier';
    RAISE NOTICE '';
    RAISE NOTICE 'Tables de dimensions:';
    RAISE NOTICE '  - dw.dim_patient';
    RAISE NOTICE '  - dw.dim_etablissement';
    RAISE NOTICE '  - dw.dim_date';
    RAISE NOTICE '  - dw.dim_service';
    RAISE NOTICE '';
    RAISE NOTICE 'Table de faits:';
    RAISE NOTICE '  - dw.fact_facturation';
    RAISE NOTICE '';
    RAISE NOTICE 'Prêt pour le pipeline ETL Airflow !';
    RAISE NOTICE '============================================================';
END $$;
