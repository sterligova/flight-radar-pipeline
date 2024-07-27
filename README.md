# FlightRadar24

# Sujet


Créer un pipeline ETL (Extract, Transform, Load) permettant de traiter les données de l'API [flightradar24](https://www.flightradar24.com/), qui répertorie l'ensemble des vols aériens, aéroports, compagnies aériennes mondiales.

> En python, cette librairie: https://github.com/JeanExtreme002/FlightRadarAPI facilite l'utilisation de l'API.

## Résultats

Ce pipeline doit permettre de fournir les indicateurs suivants:
1. La compagnie avec le + de vols en cours
2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
3. Le vol en cours avec le trajet le plus long
4. Pour chaque continent, la longueur de vol moyenne
5. L'entreprise constructeur d'avions avec le plus de vols actifs
6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage

## Industrialisation

Ce kata est orienté **industrialisation**. Le pipeline ETL doit être pensé comme un job se faisant éxécuter à échéance régulière (ex: toutes les 2 heures).

Le job doit donc être
* **fault-tolerant**: Un corner-case pas couvert ou une donnée corrompue ne doivent pas causer l'arret du job.
* **observable**: En loggant les informations pertinantes
* **systématique**: conserver les données & résultats dans un mécanisme de stockage, en adoptant une nomencalture adaptée permettant aux _data analyst_ en aval de retrouver les valeurs recherchées pour un couple `(Date, Heure)` donné.


## ⚠️ Candidatures ⚠️


> Le kata laisse volontairement beaucoup de liberté. Il y a une grande marge de progression entre un “MVP” et une implémentation “parfaite”. Au candidat de choisir sur quelles exigences mettre le focus dans son rendu.

> Le rendu MVP implémente au moins 4 des questions de l'énoncé, assorti d'un Readme expliquant la démarche choisie

> A défaut d'implémenter tout le pipeline, proposez dans le README **un exemple d'architecture idéal de votre application industrialisée**(dans un environnement de PROD) sans avoir besoin de l'implémenter (ex: ordonnancement, monitoring, data modeling, etc.)

> Pour faire ce schéma, https://www.diagrams.net/ ou https://excalidraw.com/ sont vos amis :)

> **Pour le rendu, Poussez sur une nouvelle branche git, ouvrez une merge request vers Main, et notifiez votre interlocuteur par message que le kata est fini.

![flightradarimage](media-assets/flightradar.png)


## Contexte & motivation derrière le kata


Un data engineer doit être capable de concevoir un pipeline de données pour gérer un flux important et en tirer des informations pertinentes. 


En tant que data engineer, il est important de pouvoir **explorer & comprendre le dataset qu’on manipule** pour proposer les Vues adaptées au différents use-cases, et effectuer le data-cleaning nécessaire. 

https://www.flightradar24.com/ est une API fournissant des informations **en temps réel** sur le traffic aérien mondial. De ce fait, les informations qu'elle renvoie changent en parmanence. Pour en tirer des informations utiles, son traitement doit donc **doit être répété régulièrement**. Pour des raisons d'efficacité, on cherche donc à transformer ce pipeline ETL en **un job ne requérant pas d'intervention humaine.**


## Specification [RFC2119](https://microformats.org/wiki/rfc-2119-fr) du kata


* Un grand pouvoir implique de grandes responsabilités. Vos choix `DOIVENT` être justifiés dans un Readme. 

* L'extraction des données `PEUT` être faite dans le format de votre choix. CSV, Parquet, AVRO, ... celui qu'il vous semble le plus adapté

* Votre pipeline `DOIT` inclure une phase de [data cleaning](https://fr.wikipedia.org/wiki/Nettoyage_de_donn%C3%A9es)

* Le rendu `PEUT` comporter un Jupyter notebook avec les résultats

* votre pipeline `DEVRAIT` utiliser Apache Spark et l'API DataFrame

* votre pipeline `DEVRAIT` stocker les données dans un dossier avec une nomenclature horodatée. Ex: `Flights/rawzone/tech_year=2023/tech_month=2023-07/tech_day=2023-07-16/flights2023071619203001.csv`


> Questions Bonus: Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?

# KATA - Flight Radar ETL pipeline

L'objectif principal de ce projet est de fournir un pipeline spark qui soit flexible. L'accent est mis sur la simplicité et la logique du pipeline et de ses configurations, ce qui permet de l'adapter facilement en fonction des besoins de l'utilisateur.

### Architecture du code et étapes clés

**flight_radar_peipeline**

#### [etl.py](./flight_radar_pipeline/etl.py)

Ce code implémente un pipeline ETL (Extraction, Transformation, Chargement) pour traiter et analyser des données de vol obtenues via l'API FlightRadar24

**1 Initialisation et configuration**

- Le code importe les modules nécessaires et définit des variables d'environnement pour les noms de session Spark et les répertoires de stockage des données.

**2 Fonction principale flight_radar_ETL**

- Cette fonction principale orchestre l'ensemble du processus ETL.
- l’initialisation l'API FlightRadar24 et crée une session Spark.
- l’extraction les données brutes des vols et des zones géographiques.
- la nettoyage les données des vols.
- l’exécution diverses transformations et analyses sur les données nettoyées.
- arrête la session Spark à la fin du processus.

**3 Extraction des données de vol (run_get_flights)**

- Cette fonction extrait les données de vol via l'API FlightRadar24 et crée un DataFrame Spark à partir de ces données.
- l'enregistrement les données brutes (couche bronze) dans un répertoire spécifié.
Nettoyage des données (run_get_clean_flights) :

- Cette fonction nettoie les données de vol extraites pour en améliorer la qualité.
Elle enregistre les données nettoyées (couche silver) dans un répertoire spécifié.

**4 Transformations et analyses**

4.1. Compagnie avec le plus de vols en cours (`run_most_active_airline`) :
- Identifie la compagnie aérienne ayant le plus de vols actifs.
- Enregistre les résultats (couche gold).

4.2 Compagnie avec le plus de vols régionaux actifs par continent(`run_most_active_airline_per_continent`) :
- Identifie, pour chaque continent, la compagnie aérienne ayant le plus de vols régionaux actifs.
- Enregistre les résultats (couche gold).

4.3 Vol en cours avec le trajet le plus long (`run_longest_flight`) :
- Identifie le vol en cours ayant la durée la plus longue.
- Enregistre les résultats (couche gold).

4.4 Durée moyenne des vols par continent (`run_avg_flight_duration_per_continent`) :
- Calcule la durée moyenne des vols pour chaque continent.
- Enregistre les résultats (couche gold).

4.5 Constructeur d'avions avec le plus de vols actifs (`run_aircraft_manufacturer`) :
- Identifie le constructeur d'avions ayant le plus de vols actifs.
- Enregistre les résultats (couche gold).

4.6 Top 3 des modèles d'avions en usage par compagnie aérienne (`run_top_3_aircraft_models`) :
- Identifie, pour chaque compagnie aérienne, les trois modèles d'avions les plus utilisés.
- Enregistre les résultats (couche gold).

 #### [infrastructure.py](./flight_radar_pipeline/infrastructure.py )

Сe code met en place les fondations nécessaires pour interagir avec les données de vol en utilisant Spark pour le traitement des données et FlightRadar24API pour l'extraction des données en temps réel. La fonction get_spark_session initialise une session Spark, tandis que la fonction get_flight_radar_api crée une instance de l'API FlightRadar24.

#### [extract.py](./flight_radar_pipeline/extract.py)
 
**1 Fonction get_flights**

Extrait la liste des vols en utilisant l'API FlightRadar24.

Arguments : `fr_api` (FlightRadar24API): Instance de l'API FlightRadar24.

Retourne :
Une liste de dictionnaires contenant des informations sur chaque vol, telles que l'identifiant, le numéro de vol, la latitude, la longitude, et d'autres détails.

**2 Fonction get_zones**

Extrait la liste des zones géographiques à partir de l'API FlightRadar24.

Arguments : `fr_api` (FlightRadar24API): Instance de l'API FlightRadar24.

Retourne : Un dictionnaire contenant les informations sur les zones géographiques.

#### [load.py](./flight_radar_pipeline/load.py)
 
**Fonction write_to_csv**

Écrit un DataFrame dans un fichier CSV. La fonction génère un chemin de sortie pour le fichier CSV en fonction de la date et de l'heure actuelles. Le chemin inclut des informations sur l'année, le mois, et le jour.
Le nom du fichier CSV est composé du préfixe fourni et d'un horodatage formaté.

Arguments :
- dataframe: Le DataFrame à écrire.
- folder (str): Le nom du dossier où le fichier sera enregistré.
- prefix (str): Le préfixe pour les fichiers de sortie.



#### [transform.py](./flight_radar_pipeline/extract.py)
 
1. Fonction `clean_data` :

Description : Supprime les valeurs manquantes ou indésirables du DataFrame. Les valeurs "N/A", "NaN" et les chaînes vides sont remplacées par None, puis toutes les lignes contenant des valeurs nulles sont supprimées.

2. Fonction `get_udf_continent` :

Description : Crée une fonction UDF (User Defined Function) qui détermine le continent en fonction des coordonnées géographiques (latitude et longitude). La fonction utilise un dictionnaire zones pour vérifier dans quelle zone se trouvent les coordonnées fournies.

3. Fonction `get_most_active_airline` :

Description : Identifie la compagnie aérienne avec le plus grand nombre de vols en cours (vols qui ne sont pas au sol). Le résultat est un DataFrame contenant le code ICAO de la compagnie aérienne avec le plus grand nombre de vols actifs.

4. Fonction `get_most_active_airline_per_continent` :

Description : Pour chaque continent, détermine la compagnie aérienne avec le plus grand nombre de vols régionaux actifs. Utilise une UDF pour attribuer un continent à chaque vol en fonction des coordonnées géographiques.

5. Fonction `get_longest_ongoing_flight` :

Description : Trouve le vol en cours avec la plus longue durée de trajet en se basant sur le temps de chaque vol. Le résultat est une ligne du DataFrame contenant les détails du vol le plus long.

6. Fonction `get_avg_flight_duration_per_continent` :

Description : Calcule la durée moyenne des vols pour chaque continent. La fonction attribue d'abord un continent à chaque vol en utilisant une UDF, puis calcule la durée moyenne des vols pour chaque continent.

7. Fonction `get_aircraft_manufacturer_with_most_active_flights` :

Description : Identifie le constructeur d'avions avec le plus grand nombre de vols actifs (vols qui ne sont pas au sol). Le résultat est une ligne du DataFrame contenant le code de l'avion du constructeur avec le plus grand nombre de vols actifs.

8. Fonction `get_top_3_aircraft_models_per_airline` :

Description : Pour chaque pays de la compagnie aérienne, détermine les trois modèles d'avions les plus utilisés. Le résultat est un DataFrame listant les trois principaux modèles d'avions pour chaque compagnie aérienne, triés par nombre d'occurrences.
 
## Industrialisation

 Le pipeline ETL est construit pour executer dans un job [scheduler.py](./scheduler.py), pour configure il faut utiliser la variable d’enivrement `SCHEDULER_INTERVAL_MIN`

Le job
* **fault-tolerant**: try / except pattern
* **observable**: python `logging` library
* **systématique**: Les données finales seront stockées au format CSV. Le dossier `output` est structuré de manière à ce qu'à l'intérieur, le dossier `stg` puisse stocker les `.csv` bruts, le dossier `ods` (operational data store) puisse stocker les `.csv` nettoyés, et le dossier `cdm` (common data mart layer) puisse stocker les vues métiers `.csv`.

## Dev Requirements

- java11
- hadoop3
- spark3.5.1
- python3.12 

## Prepare env

Create venv:

```bash
 python -m venv ./venv
 ```

 Activate venv:

 ```bash
 source .venv/bin/activate
 ```

 Install packages

 ```bash
 pip install -r requirements.txt
 ```

Linter

 ```bash
 black ./flight_radar_pipeline
 ```

 ## Execute test pipeline

 ```bash
 python3 ./run.py
 ```

  ## Start Scheduler

 ```bash
 python3 ./scheduler.py
 ```