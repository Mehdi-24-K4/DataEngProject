# Projet ETL pour l'Analyse des Données de la Qualité de l'Air

Ce projet met en place un pipeline ETL (Extract, Transform, Load) pour l'analyse des données de la qualité de l'air en utilisant l'API OpenAQ. Les données sont extraites, transformées et chargées en utilisant plusieurs technologies modernes et conteneurs Docker.

## Description

Le projet est composé de plusieurs scripts Python pour gérer différentes étapes du pipeline ETL. Les étapes principales comprennent :
1. **Extraction des données** : Les données sont extraites de l'API OpenAQ et envoyées à un broker Kafka.
2. **Consommation des données** : Les données sont consommées depuis Kafka, stockées dans MongoDB, puis transformées et agrégées en utilisant Apache Spark.
3. **Chargement des données** : Les données transformées sont chargées dans une base de données PostgreSQL avec un schéma en étoile.

## Technologies Utilisées

- **Kafka** : Broker de messages pour le traitement des flux de données.
- **Zookeeper** : Service de coordination pour Kafka.
- **Schema Registry** : Gestion des schémas de données pour Kafka.
- **MongoDB** : Base de données NoSQL pour le stockage temporaire des données.
- **Apache Spark** : Framework de traitement de données pour la transformation et l'agrégation.
- **PostgreSQL** : Base de données relationnelle pour le stockage final des données avec un schéma en étoile.
- **Airflow** : Orchestrateur de workflows pour la gestion des tâches ETL.
- **Grafana** : Outil de visualisation pour les métriques et les dashboards.
- **Docker** : Conteneurisation pour isoler les services.

## Prérequis

- [Docker](https://www.docker.com/) et [Docker Compose](https://docs.docker.com/compose/)
- Python 3.x
- Accès à l'API OpenAQ (pour extraire les données)

## Installation

1. **Clonez le dépôt** :
    ```bash
    git clone https://github.com/username/repository.git
    cd repository
    ```

2. **Démarrez les conteneurs Docker** :
    Utilisez Docker Compose pour démarrer les services nécessaires :
    ```bash
    docker-compose up --build
    ```

## Utilisation

### 1. Extraction des données
Accéder à l'interface d'Airflow via localhost:8084/
Lancer le dag
