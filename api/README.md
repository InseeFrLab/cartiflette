# API GeoJSON de cartiflette

Sert du GeoJSON à partir des GeoParquet consolidés (un fichier par niveau), à la
place des fichiers GeoJSON publiés par valeur (`DEPARTEMENT=75`, `REGION=11`…).
Le pipeline n'aurait plus à produire ni à téléverser un fichier par valeur de
découpage.

Elle réutilise le client (`cartiflette.client.read_parquet`) : DuckDB ne
télécharge que les row groups des valeurs demandées, puis la sérialisation en
GeoJSON se fait en SQL (`ST_AsGeoJSON`), en streaming, avec une compression gzip.

## Routes

- `GET /v1/geojson?year=2025&borders=COMMUNE&filter_by=DEPARTEMENT&values=75&values=92`
  : plusieurs valeurs dans une seule réponse. Mêmes paramètres que
  `carti_download` (`crs`, `simplification`, `path_within_bucket`, par défaut
  `production`).
- `GET /{chemin d'un fichier GeoJSON du stockage}`, par exemple
  `/projet-cartiflette/production/provider=IGN/.../DEPARTEMENT=75/vectorfile_format=geojson/territory=metropole/simplification=50/raw.geojson`
  : un client qui lit les fichiers n'a qu'à changer d'hôte.
- `GET /health`.

Pour un millésime sans GeoParquet (2022), les deux routes redirigent (307) vers
le fichier GeoJSON ; `/v1/geojson` avec plusieurs valeurs lit les fichiers et les
renvoie fusionnés.

Erreurs : 404 si un fichier n'existe pas, si le niveau ne peut pas être filtré
par `filter_by` ou si une valeur est introuvable ; 400 pour un
`path_within_bucket` invalide.

## Lancer

```bash
cd api
uv run uvicorn --factory cartiflette_api.app:create_app --port 8000
uv run pytest tests                      # tests unitaires, sans réseau
```

Les tests d'intégration du client (cas d'usage du site) passent par l'API avec
`CARTIFLETTE_API_URL` :

```bash
cd python-package/cartiflette
CARTIFLETTE_API_URL=http://localhost:8000 uv run pytest -m integration
```

Résultat : 91 réussis, 5 ignorés (options propres au client Python : `force`,
`engine="duckdb"`, topojson), 2 xfail (comme sans API).

## Image Docker

À construire depuis la racine du dépôt (l'API dépend du client) :

```bash
docker build -f api/Dockerfile -t cartiflette-api .
docker run -p 8000:8000 cartiflette-api
```

Publiée par la CI (`docker.yml`) sous `inseefrlab/cartiflette-api:v<version>`,
version de `api/pyproject.toml` : la monter avant de pousser sur `main`, sinon
l'image est écrasée.

## Coût par rapport aux fichiers

`benchmark.py` rejoue les cas d'usage des tests d'intégration (millésime 2025,
`test/v0.2.0`, résultats bruts dans `benchmark-2025.json`) :

```bash
uv run python benchmark.py --repeat 5 --output benchmark-2025.json
```

Mesuré depuis un pod SSPCloud, donc dans le même réseau que MinIO (médianes de
5 essais). « Froid » : première requête après le démarrage de l'API ; « chaud » :
les suivantes, DuckDB gardant en cache les parties de fichiers déjà lues.
« Texte » : le GeoJSON reçu, comme pour un client R ou JS ; « GeoDataFrame » : le
texte lu par `geopandas.read_file`.

| Scénario | Lignes | Fichiers | Mo GeoJSON | Mo transférés API | Texte : fichiers | API froid | API chaud | GeoDataFrame : fichiers | API froid | API chaud | GeoParquet |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| usecase1 arrondissements petite couronne | 142 | 4 | 0.24 | 0.07 | 0.03 s | 0.24 s | 0.12 s | 0.07 s | 0.31 s | 0.14 s | 0.32 s |
| usecase1 arrondissements Lyon | 274 | 1 | 0.69 | 0.20 | 0.03 s | 0.24 s | 0.12 s | 0.07 s | 0.30 s | 0.18 s | 0.29 s |
| usecase2 departements DROM rapproches | 105 | 1 | 15.07 | 5.29 | 0.10 s | 0.69 s | 0.57 s | 0.81 s | 1.40 s | 1.30 s | 0.50 s |
| home communes 13 | 119 | 1 | 0.44 | 0.15 | 0.03 s | 0.22 s | 0.13 s | 0.07 s | 0.25 s | 0.18 s | 0.27 s |
| home communes 01 + 74 | 670 | 2 | 1.98 | 0.62 | 0.03 s | 0.28 s | 0.17 s | 0.18 s | 0.43 s | 0.32 s | 0.33 s |
| home communes 13 (simplification 0) | 119 | 1 | 0.91 | 0.32 | 0.03 s | 0.27 s | 0.14 s | 0.10 s | 0.36 s | 0.24 s | 0.29 s |
| home regions France | 18 | 1 | 5.97 | 2.28 | 0.05 s | 0.43 s | 0.29 s | 0.45 s | 0.87 s | 0.67 s | 0.36 s |
| home bassins de vie France (simplification 0) | 1707 | 1 | 50.00 | 18.93 | 0.25 s | 2.05 s | 1.67 s | 3.68 s | 5.38 s | 5.04 s | 1.56 s |
| home AAV DROM rapproches (simplification 0) | 705 | 1 | 62.56 | 21.43 | 0.29 s | 2.51 s | 2.11 s | 3.42 s | 5.68 s | 5.16 s | 1.58 s |

Dans le même réseau, l'API coûte **+0,1 à 0,25 s** pour une requête courante
(communes d'un ou deux départements) et **+1,5 à 2,2 s** pour un niveau France
entière de 50 à 60 Mo. Sur ces gros cas, le temps se répartit entre la lecture du
GeoParquet (~0,4 s), la sérialisation SQL (~0,5 s) et le gzip (~0,8 s au niveau 1 ;
le niveau 5 prenait 2,5 s pour 11 % d'octets en moins, d'où le niveau 1).

Pour un client distant, c'est le transfert qui domine. MinIO sert les fichiers
sans compression, l'API en gzip : trois fois moins d'octets. Estimation (temps
mesuré ci-dessus + octets transférés au débit indiqué, API à froid) :

| Scénario | Fichiers 20 Mbit/s | API 20 Mbit/s | Fichiers 100 Mbit/s | API 100 Mbit/s |
|---|---:|---:|---:|---:|
| usecase1 arrondissements petite couronne | 0.13 s | 0.27 s | 0.05 s | 0.25 s |
| usecase1 arrondissements Lyon | 0.30 s | 0.32 s | 0.08 s | 0.25 s |
| usecase2 departements DROM rapproches | 6.12 s | 2.81 s | 1.30 s | 1.11 s |
| home communes 13 | 0.20 s | 0.27 s | 0.06 s | 0.23 s |
| home communes 01 + 74 | 0.82 s | 0.53 s | 0.19 s | 0.33 s |
| home communes 13 (simplification 0) | 0.39 s | 0.39 s | 0.10 s | 0.29 s |
| home regions France | 2.44 s | 1.34 s | 0.53 s | 0.61 s |
| home bassins de vie France (simplification 0) | 20.25 s | 9.62 s | 4.25 s | 3.56 s |
| home AAV DROM rapproches (simplification 0) | 25.31 s | 11.08 s | 5.29 s | 4.22 s |

Ce gain vient de la compression, pas de l'API : des fichiers GeoJSON stockés
avec `Content-Encoding: gzip` l'auraient aussi. L'API ne le paie pas en octets,
seulement en latence fixe (~0,2 s) sur les petites requêtes.

Pour le client Python, le GeoParquet lu directement reste le plus rapide sur les
gros niveaux (1,6 s contre 5 s via l'API) : l'API sert les clients qui veulent du
GeoJSON (R, JS, navigateur), pas `carti_download`.

24 requêtes simultanées (communes de 12 départements, deux fois) : 0,7 s au total,
un curseur DuckDB par requête sur une connexion partagée.
