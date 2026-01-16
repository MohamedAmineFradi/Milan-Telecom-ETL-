# Milan Telecom ETL 

Projet ETL pour charger et analyser les données télécom de Milan dans une base PostgreSQL/PostGIS.

## 🎯 Fonctionnalités

- Création automatique de la base de données PostgreSQL
- Activation de l'extension PostGIS
- Chargement des géométries (grille de Milan + provinces italiennes)
- Import des données de trafic (SMS, appels, internet)
- Import des données de mobilité inter-provinces
- Requêtes d'analyse et KPIs

## 📋 Prérequis

- Python 3.8+
- PostgreSQL 12+ avec PostGIS

## 🚀 Installation

### 1. Cloner le projet et installer les dépendances

```bash
cd telecom_milan_etl
pip install -r requirements.txt
```

### 2. Configuration

Copier le fichier `.env.example` en `.env` et configurer vos paramètres:

```bash
cp .env.example .env
```

Éditer `.env` avec vos paramètres:

```env
DB_NAME=milan_telecom
DB_USER=votre_utilisateur
DB_PASSWORD=votre_mot_de_passe
DB_HOST=localhost
DB_PORT=5432
DATA_DIR=./data_milan_cdr_kaggle
```

## 📊 Structure du projet

```
telecom_milan_etl/
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── database.py
│   └── etl.py
├── main.py
├── requirements.txt
├── .env
├── .env.example
├── .gitignore
└── data_milan_cdr_kaggle/
    ├── milano-grid.geojson
    ├── Italian_provinces.geojson
    ├── sms-call-internet-mi-*.csv
    └── mi-to-provinces-*.csv
```

## 💻 Utilisation

### Pipeline complet (recommandé pour la première fois)

```bash
python main.py --all
```

Ceci va:
1. Créer la base de données et le schéma
2. Charger les géométries
3. Charger toutes les données CSV
4. Exécuter une requête de test

### Limiter le nombre de fichiers (pour test rapide)

```bash
python main.py --all --limit-files 3
```

### Étapes individuelles

```bash
# Créer uniquement la base et le schéma
python main.py --setup

# Charger uniquement les géométries
python main.py --load-geo

# Charger uniquement les données CSV
python main.py --load-data

# Charger 3 premiers fichiers seulement
python main.py --load-data --limit-files 3

# Exécuter une requête de test
python main.py --test
```

## 🗄️ Schéma de la base de données

### Tables de dimensions

- **dim_grid_milan**: Grille spatiale de Milan (10 000 cellules)
- **dim_provinces_it**: Provinces italiennes avec géométries

### Tables de faits

- **fact_traffic_milan**: Trafic télécom (SMS, appels, internet) par heure et cellule
- **fact_mobility_provinces**: Flux de mobilité entre Milan et les provinces

### Vues

- **v_hourly_traffic**: Agrégation horaire du trafic total par cellule



