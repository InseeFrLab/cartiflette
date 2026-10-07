# Changelog de `cartiflette-api`

API FastAPI qui sert le GeoJSON à partir des GeoParquet publiés
(<https://cartiflette-api.lab.sspcloud.fr>). Chaque version est publiée comme
image `inseefrlab/cartiflette-api:v<version>`, et elle est déployée quand son
tag est reporté dans `api/deployment/deployment.yaml`.

## 0.2.0 (2026-10-07)

### Ajouté

- Paramètre `where` sur `/v1/geojson` et `/v1/geoparquet` : le filtre de
  `carti_download` (expression SQL DuckDB sur les attributs ou la géométrie),
  appliqué avant la conversion en GeoJSON (#112). Exemple :
  `/v1/geojson?year=2026&borders=COMMUNE&filter_by=REGION&values=76&where=POPULATION > 2000`.
- L'expression vient de n'importe qui : `check_where` la vérifie avant de
  l'exécuter, sur l'arbre syntaxique de DuckDB. Il faut une seule expression,
  sans `FROM`, sous-requête ni autre clause. Seules sont permises les
  fonctions spatiales (`ST_*`) et celles de `WHERE_FUNCTIONS`. Sinon,
  l'API répond 400, comme pour une expression invalide ou qui échoue sur
  les données.
- 2022 (GeoJSON seul) : avec `where`, la requête n'est pas redirigée ; le
  fichier est lu puis filtré.

### Modifié

- Repose sur le client 0.4.0. La lecture du GeoParquet reste paresseuse, donc
  le filtre s'applique pendant la lecture. Les distances et reprojections
  lisent les coordonnées en (longitude, latitude).

## 0.1.4 (2026-10-06)

### Modifié

- **Simplification par défaut à 80** au lieu de 0, comme le client 0.3.0.
  Pour les GeoJSON de 2022, qui n'ont pas de version 80, le défaut est 50.
  Il faut les fichiers du _pipeline_ 0.4.0.

## 0.1.3 (2026-10-05)

### Ajouté

- Route `/v1/geoparquet` : mêmes paramètres que `/v1/geojson`. Elle renvoie un
  fichier GeoParquet limité aux polygones demandés, pour les millésimes
  publiés en GeoParquet.
- Paramètre `download=1` sur `/v1/geojson` : le fichier est téléchargé au lieu
  d'être affiché.
- Fichiers nommés d'après le niveau et le millésime, comme ceux de l'Insee, et
  non plus `raw` : `DEP2026.geojson`, `BV2023.parquet`, `COMARM2026.geojson`.
- 2022 (sans GeoParquet) : avec `download=1`, la requête n'est pas redirigée,
  car une redirection ne peut pas nommer le fichier ; le fichier est lu puis
  renvoyé.

## 0.1.2 (2026-10-04)

### Corrigé

- Fusion des GeoJSON de 2022 : la mémoire nécessaire suit la taille des
  fichiers (client 0.2.1) au lieu d'environ 4 Go, ce qui dépassait la limite
  des pods (2 Gio). Un fichier manquant renvoie une 404.

## 0.1.1 (2026-10-04)

### Modifié

- Image reconstruite avec le client 0.2.1, quand le _pipeline_ 0.3.0 a cessé
  de produire du GeoJSON. Les routes ne changent pas.

## 0.1.0 (2026-10-02)

Première version.

### Ajouté

- `/v1/geojson` : les paramètres de `carti_download`, avec plusieurs valeurs
  dans une seule réponse. Le GeoJSON est lu dans le GeoParquet et envoyé
  entité par entité, compressé en gzip.
- Chemins des fichiers GeoJSON du stockage S3
  (`/projet-cartiflette/production/provider=IGN/.../raw.geojson`) : un client
  qui lit ces fichiers n'a qu'à changer d'hôte.
- Millésimes sans GeoParquet (2022) : redirection (307) vers le fichier
  GeoJSON. Avec plusieurs valeurs, les fichiers sont lus et fusionnés.
- `/health`, image Docker (`api/Dockerfile`), déploiement avec Argo CD
  (`api/deployment/`, `api/application.yaml`).
- Mesures de coût par rapport aux fichiers (`benchmark.py`, `README.md`).
