# Bienvenue dans le projet de data engineering de Denis Desreumaux pour Polytech Lille !

Ce document va vous permettre de pouvoir faire fonctionner ce projet autour des données sur les stations de vélos en libre-service de France.

Le sujet de ce projet est situé à l'adresse suivante [Sujet du projet](https://github.com/kevinl75/polytech-de-101-2024-tp-subject)

# Villes incluses dans le projet

Voici la liste des villes dont les données ont été récupérées pour ce projet :

- [Open Data Paris](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/table/)

- [Open Data Nantes](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)

- [Open data Toulouse](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)

- [Open data Strasbourg](https://data.strasbourg.eu/explore/dataset/stations-velhop/table/)

- [Open data Montpellier, endpoint bikestation](https://portail-api.montpellier3m.fr/)

# Préparation du terminal

Avant de lancer les commandes python des parties suivantes, il vous faut préparer un terminal. Pour cela, clonez le projet et exécutez les commandes suivantes à la racine du projet : 

```python
python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```

(.venv) devrait être affiché au début de votre prompt.

# Lancer l'ingestion

Il vous faut taper les commandes suivantes à partir de la racine du projet : 

```python
python src/main.py
```

# Tester le projet

Pour exécuter la requpete de test sur le nombre d'emplacements disponibles de vélos dans une ville, à savoir : 

```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse', 'strasbourg', 'montpellier');
```

vous pouvez exécuter la commande suivante, après avoir lancer au moins une fois l'ingestion des données : 

```python
python tests/getSumDocksAvailablePerCity.py
```

A noter que la requête a légèrement changé par rapport à la requête originale puisque les villes de Strasbourg et de Montpellier ont été ajoutées. 

Pour exécuter la deuxième requête de test pour avoir la moyenne du nombre de vélos par station, à savoir : 
```sql
-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

vous pouvez exécuter la commande suivante, là aussi seulement si au moins une ingestion des données a été effectuée : 

```python
python tests/getAvgDockAvailablePerStation.py
```

Vous pouvez aussi supprimer l'intégralité des tables du projet en effectuant la commande suivante : 

```python
python setup/deleteAllTables.py
```

Attention, après avoir supprimé les tables, il faudra faire une nouvelle ingestion afin de pouvoir exécuter les deux autres commades de test.

# Différences entre le sujet et ce projet

La structure des tables ainsi que la structure des fichiers n'ont pas changé. Cependant pour Starsbourg, le nom de la ville a été écrit en dur dans le code pour le stocker dans la colonne _city_name_ de la table _CONSOLIDATE_STATION_ car cette information n'était pas disponible via leur API.

# Autre site similaire

Un autre site recense les stations de centaines de villes. Il s'agit de [citybik.es](citybik.es). L'avantage avec ce site est d'avoir la possibilité d'avoir accès aux données de plus de villes du monde, mais dans le cadre du projet, on perd la notion de devoir appeler au moins 2 API différentes concernant les stations. 
