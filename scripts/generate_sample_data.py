"""
Script de génération de données hétérogènes pour simuler 5 établissements de santé
avec des formats CSV/Excel différents (colonnes, séparateurs, dates).

Usage:
    python scripts/generate_sample_data.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random

# Configuration
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
NUM_RECORDS_PER_ETABLISSEMENT = 200
SEED = 42

# Initialiser le générateur aléatoire
np.random.seed(SEED)
random.seed(SEED)


def generate_base_data(num_records: int, etablissement_id: str) -> pd.DataFrame:
    """Génère un DataFrame de base avec des données de facturation."""
    
    # Dates de facturation (3 derniers mois)
    start_date = datetime.now() - timedelta(days=90)
    dates_facturation = [
        start_date + timedelta(days=random.randint(0, 90))
        for _ in range(num_records)
    ]
    
    # Statuts de paiement
    statuts = ['PAYE', 'PARTIEL', 'IMPAYE', 'EN_COURS']
    statuts_paiement = np.random.choice(statuts, num_records, p=[0.6, 0.15, 0.15, 0.1])
    
    # Montants
    montants_facture = np.random.uniform(500, 15000, num_records).round(2)
    
    # Calcul montant payé selon statut
    montants_paye = []
    for i, statut in enumerate(statuts_paiement):
        if statut == 'PAYE':
            montants_paye.append(montants_facture[i])
        elif statut == 'PARTIEL':
            montants_paye.append(round(montants_facture[i] * random.uniform(0.3, 0.8), 2))
        else:
            montants_paye.append(0.0)
    
    # Dates de paiement (si payé)
    dates_paiement = []
    for i, statut in enumerate(statuts_paiement):
        if statut in ['PAYE', 'PARTIEL']:
            delai = timedelta(days=random.randint(1, 60))
            dates_paiement.append(dates_facturation[i] + delai)
        else:
            dates_paiement.append(None)
    
    # Services médicaux
    services = ['URGENCES', 'CHIRURGIE', 'CARDIOLOGIE', 'PEDIATRIE', 'RADIOLOGIE', 'MATERNITE']
    
    # Générer les données
    data = {
        'patient_id': [f"PAT{etablissement_id}{str(i+1).zfill(5)}" for i in range(num_records)],
        'patient_nom': [f"Patient_{random.randint(1000, 9999)}" for _ in range(num_records)],
        'patient_prenom': [f"Prenom_{random.randint(100, 999)}" for _ in range(num_records)],
        'etablissement_id': [etablissement_id] * num_records,
        'etablissement_nom': [f"Clinique {etablissement_id}"] * num_records,
        'service': np.random.choice(services, num_records),
        'date_facturation': dates_facturation,
        'montant_facture': montants_facture,
        'statut_paiement': statuts_paiement,
        'montant_paye': montants_paye,
        'date_paiement': dates_paiement,
        'nombre_lignes': np.random.randint(1, 20, num_records),
    }
    
    return pd.DataFrame(data)


def create_etablissement1_csv(df: pd.DataFrame, output_dir: Path):
    """
    Établissement 1: Format CSV classique avec virgule
    Noms de colonnes en français avec majuscules
    """
    df_etab = df.copy()
    df_etab = df_etab.rename(columns={
        'patient_id': 'NumPatient',
        'patient_nom': 'NomPatient',
        'patient_prenom': 'PrenomPatient',
        'etablissement_id': 'CodeEtablissement',
        'etablissement_nom': 'NomEtablissement',
        'service': 'ServiceMedical',
        'date_facturation': 'DateFacture',
        'montant_facture': 'Montant',
        'statut_paiement': 'StatutPaiement',
        'montant_paye': 'MontantPaye',
        'date_paiement': 'DatePaiement',
        'nombre_lignes': 'NombreLignes'
    })
    
    # Format date français
    df_etab['DateFacture'] = df_etab['DateFacture'].dt.strftime('%d/%m/%Y')
    df_etab['DatePaiement'] = df_etab['DatePaiement'].apply(
        lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else ''
    )
    
    filepath = output_dir / "etablissement1_facturation.csv"
    df_etab.to_csv(filepath, index=False, sep=',', encoding='utf-8')
    print(f" Créé: {filepath}")


def create_etablissement2_csv(df: pd.DataFrame, output_dir: Path):
    """
    Établissement 2: CSV avec point-virgule
    Noms de colonnes en anglais
    Format date US
    """
    df_etab = df.copy()
    df_etab = df_etab.rename(columns={
        'patient_id': 'PatientID',
        'patient_nom': 'LastName',
        'patient_prenom': 'FirstName',
        'etablissement_id': 'FacilityCode',
        'etablissement_nom': 'FacilityName',
        'service': 'Department',
        'date_facturation': 'BillingDate',
        'montant_facture': 'InvoiceAmount',
        'statut_paiement': 'PaymentStatus',
        'montant_paye': 'PaidAmount',
        'date_paiement': 'PaymentDate',
        'nombre_lignes': 'LineItems'
    })
    
    # Format date US
    df_etab['BillingDate'] = df_etab['BillingDate'].dt.strftime('%Y-%m-%d')
    df_etab['PaymentDate'] = df_etab['PaymentDate'].apply(
        lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
    )
    
    filepath = output_dir / "etablissement2_billing.csv"
    df_etab.to_csv(filepath, index=False, sep=';', encoding='utf-8')
    print(f" Créé: {filepath}")


def create_etablissement3_csv(df: pd.DataFrame, output_dir: Path):
    """
    Établissement 3: CSV avec tabulations
    Noms de colonnes abrégés
    Format date ISO
    """
    df_etab = df.copy()
    df_etab = df_etab.rename(columns={
        'patient_id': 'ID_Pat',
        'patient_nom': 'Nom',
        'patient_prenom': 'Prenom',
        'etablissement_id': 'Etab_Code',
        'etablissement_nom': 'Etab_Nom',
        'service': 'Serv',
        'date_facturation': 'Dt_Fact',
        'montant_facture': 'Mt_Fact',
        'statut_paiement': 'Statut',
        'montant_paye': 'Mt_Paye',
        'date_paiement': 'Dt_Paye',
        'nombre_lignes': 'Nb_Lignes'
    })
    
    # Format date ISO
    df_etab['Dt_Fact'] = df_etab['Dt_Fact'].dt.strftime('%Y-%m-%d')
    df_etab['Dt_Paye'] = df_etab['Dt_Paye'].apply(
        lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
    )
    
    filepath = output_dir / "etablissement3_data.csv"
    df_etab.to_csv(filepath, index=False, sep='\t', encoding='utf-8')
    print(f" Créé: {filepath}")


def create_etablissement4_excel(df: pd.DataFrame, output_dir: Path):
    """
    Établissement 4: Fichier Excel
    Noms de colonnes mixtes (FR/EN)
    Format date mixte
    """
    df_etab = df.copy()
    df_etab = df_etab.rename(columns={
        'patient_id': 'ID_Patient',
        'patient_nom': 'NomFamille',
        'patient_prenom': 'Prenom',
        'etablissement_id': 'Hospital_ID',
        'etablissement_nom': 'Hospital_Name',
        'service': 'Service',
        'date_facturation': 'Invoice_Date',
        'montant_facture': 'Total_Amount',
        'statut_paiement': 'Status',
        'montant_paye': 'Amount_Paid',
        'date_paiement': 'Payment_Date',
        'nombre_lignes': 'Item_Count'
    })
    
    # Garder les dates en format datetime pour Excel
    # Excel les affichera correctement
    
    filepath = output_dir / "etablissement4_invoices.xlsx"
    df_etab.to_excel(filepath, index=False, engine='openpyxl')
    print(f" Créé: {filepath}")


def create_etablissement5_csv(df: pd.DataFrame, output_dir: Path):
    """
    Établissement 5: CSV avec virgule
    Colonnes dans un ordre différent
    Noms très différents
    """
    df_etab = df.copy()
    
    # Réorganiser et renommer
    df_etab = df_etab[[
        'montant_facture', 'montant_paye', 'service', 
        'date_facturation', 'date_paiement', 'statut_paiement',
        'patient_id', 'patient_nom', 'patient_prenom',
        'etablissement_id', 'etablissement_nom', 'nombre_lignes'
    ]].rename(columns={
        'patient_id': 'ref_patient',
        'patient_nom': 'family_name',
        'patient_prenom': 'given_name',
        'etablissement_id': 'clinic_ref',
        'etablissement_nom': 'clinic_label',
        'service': 'medical_dept',
        'date_facturation': 'billing_timestamp',
        'montant_facture': 'billed_amt',
        'statut_paiement': 'payment_state',
        'montant_paye': 'received_amt',
        'date_paiement': 'received_date',
        'nombre_lignes': 'line_count'
    })
    
    # Format date avec heures (timestamp)
    df_etab['billing_timestamp'] = df_etab['billing_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_etab['received_date'] = df_etab['received_date'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
    )
    
    filepath = output_dir / "etablissement5_records.csv"
    df_etab.to_csv(filepath, index=False, sep=',', encoding='utf-8')
    print(f" Créé: {filepath}")


def main():
    """Fonction principale."""
    print("=" * 70)
    print("Génération des données de test hétérogènes")
    print("=" * 70)
    
    # Créer le répertoire de sortie
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nRépertoire de sortie: {OUTPUT_DIR}\n")
    
    # Établissement 1
    print("Génération Établissement 1 (CSV français, séparateur virgule)...")
    df1 = generate_base_data(NUM_RECORDS_PER_ETABLISSEMENT, "ETAB001")
    create_etablissement1_csv(df1, OUTPUT_DIR)
    
    # Établissement 2
    print("\nGénération Établissement 2 (CSV anglais, séparateur point-virgule)...")
    df2 = generate_base_data(NUM_RECORDS_PER_ETABLISSEMENT, "ETAB002")
    create_etablissement2_csv(df2, OUTPUT_DIR)
    
    # Établissement 3
    print("\nGénération Établissement 3 (CSV abrégé, séparateur tabulation)...")
    df3 = generate_base_data(NUM_RECORDS_PER_ETABLISSEMENT, "ETAB003")
    create_etablissement3_csv(df3, OUTPUT_DIR)
    
    # Établissement 4
    print("\nGénération Établissement 4 (Excel, colonnes mixtes)...")
    df4 = generate_base_data(NUM_RECORDS_PER_ETABLISSEMENT, "ETAB004")
    create_etablissement4_excel(df4, OUTPUT_DIR)
    
    # Établissement 5
    print("\nGénération Établissement 5 (CSV ordre différent, timestamps)...")
    df5 = generate_base_data(NUM_RECORDS_PER_ETABLISSEMENT, "ETAB005")
    create_etablissement5_csv(df5, OUTPUT_DIR)
    
    print("\n" + "=" * 70)
    print(f" Génération terminée ! {5 * NUM_RECORDS_PER_ETABLISSEMENT} lignes créées")
    print("=" * 70)
    
    # Statistiques
    total_records = NUM_RECORDS_PER_ETABLISSEMENT * 5
    print(f"\n Statistiques:")
    print(f"   - Nombre total d'enregistrements: {total_records}")
    print(f"   - Nombre d'établissements: 5")
    print(f"   - Formats générés: CSV (4) + Excel (1)")
    print(f"   - Séparateurs: virgule, point-virgule, tabulation")
    print(f"   - Formats de date: FR (dd/mm/yyyy), US (yyyy-mm-dd), ISO + timestamp")
    
    print(f"\n Prochaine étape:")
    print(f"   Lancer le DAG Airflow pour traiter ces fichiers !")


if __name__ == "__main__":
    main()
