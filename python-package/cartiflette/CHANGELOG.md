# Changelog du client `cartiflette`

Client Python publié sur [PyPI](https://pypi.org/project/cartiflette/). Le
_pipeline_ et l'API ont leur propre changelog (`CHANGELOG.md` à la racine du
dépôt, `api/CHANGELOG.md`).

## 0.4.0 (non publiée)

### Ajouté

- `borders="IRIS"` (à partir de 2025, fichiers du _pipeline_ ≥ 0.4.0), filtrable
  notamment par commune : `filter_by="COMMUNE", values="34172"`. Aucun
  changement de code : la docstring de `carti_download` le mentionne.

- Argument `where` de `carti_download` : expression SQL DuckDB qui ne garde
  qu'une partie des polygones de `values` (#112). Elle porte sur les attributs
  (`where="POPULATION > 2000"`) ou sur la géométrie, avec les fonctions spatiales
  de DuckDB : emprise (`ST_Intersects(geometry, ST_MakeEnvelope(...))`),
  distance (`ST_DWithin`, `ST_Distance_Sphere`), point dans un polygone
  (`ST_Contains`). Le filtre est appliqué avant la conversion, quels que soient
  le format et `engine` ; sur le GeoParquet, DuckDB l'applique pendant la
  lecture. Une expression invalide lève une `ValueError`.
- Fonction `client.filter_relation`, qui applique ce filtre à une relation.

### Modifié

- La connexion DuckDB active `geometry_always_xy` : `ST_Transform` depuis
  EPSG:4326 et `ST_Distance_Sphere` lisent les coordonnées en (longitude,
  latitude), l'ordre des fichiers. C'est vrai aussi pour une connexion passée
  par `con`.

### Corrigé

- `read_parquet` rendait une relation déjà exécutée : tout ce qui
  correspondait à `values` était téléchargé, même avec `engine="duckdb"`,
  documenté comme « pas encore évalué ». La relation reste maintenant
  paresseuse, et un filtre ajouté ensuite s'applique pendant la lecture. Par
  exemple, les communes d'une emprise dans le fichier France entière passent
  de 2,5 s à 0,3 s.

## 0.3.0 (2026-10-06)

### Modifié

- **Simplification par défaut à 80** (part des points retirés) au lieu de 0.
  Il faut les fichiers du _pipeline_ 0.4.0 : sans eux, un appel sans
  `simplification` explicite échoue.
- Pour les millésimes publiés seulement en GeoJSON (2022), qui n'ont pas de
  version 80, le défaut est 50.

## 0.2.1 (2026-10-04)

### Modifié

- Aucun GeoJSON n'est publié à partir de 2025 : `force=True` ne s'applique
  plus qu'aux millésimes qui ont des fichiers GeoJSON (2022). Sinon, le
  GeoParquet est lu malgré `force=True`, avec un avertissement.
- Nouvelle fonction `geojson_available`, qui vérifie l'existence des fichiers
  GeoJSON par une requête HEAD pour les millésimes antérieurs à 2025.

### Corrigé

- Lecture des GeoJSON : `maximum_object_size` est calculé d'après la taille
  réelle des fichiers (requêtes HEAD) au lieu d'être fixé à 2 Go. DuckDB
  allouait environ deux fois cette valeur et manquait de mémoire dans les
  petits conteneurs.

## 0.2.0 (2026-09-26)

Réécriture complète du client, qui lit les fichiers du nouveau _pipeline_.

### Ajouté

- Tout le traitement passe par DuckDB.
  - **GeoParquet** : un seul fichier par niveau, filtré en SQL. Seuls les
    groupes de lignes utiles sont téléchargés (environ 2 Mo pour les
    communes d'un département).
  - **GeoJSON** : les fichiers, un par valeur, sont lus ensemble.
- Le GeoParquet est lu dès qu'il existe : toujours à partir de 2025, et après
  une vérification par requête HEAD (`parquet_available`) pour les années
  antérieures. Il est lu même si `vectorfile_format="geojson"` est demandé,
  avec un avertissement. `force=True` lit quand même le GeoJSON.
- `engine="duckdb"` renvoie la relation DuckDB au lieu d'un `GeoDataFrame` ;
  `con` permet de passer sa propre connexion.
- Codes de région : `1`, `"1"` et `"01"` sont acceptés.
- Paramètres `bucket` et `path_within_bucket`, pour lire un emplacement de
  test.

### Modifié

- Python 3.10 ou plus, DuckDB 1.5 ou plus. Dépendances : `duckdb`,
  `geopandas`, `pyarrow` (plus de `requests`, `requests-cache`, `tqdm`,
  `platformdirs`).
- Le proxy est lu dans la variable d'environnement `https_proxy`.

### Supprimé

- Format `topojson` : une erreur explicite indique les formats acceptés
  (`geojson`, `parquet`).
- Cache persistant des fichiers téléchargés : le GeoParquet ne rapatrie que ce
  qui est demandé.

## 0.1.x (2025)

Client léger publié sur PyPI depuis `python-package/` (0.1.0 à 0.1.9). Il lit
les fichiers GeoJSON ou TopoJSON publiés une valeur par fichier (2022), avec
`requests` et un cache local. Ces versions ne savent pas lire les millésimes
2023 et suivants, publiés en GeoParquet.
