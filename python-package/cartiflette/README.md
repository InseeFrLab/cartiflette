# cartiflette <img src="https://raw.githubusercontent.com/InseeFrLab/cartiflette/main/cartiflette.png" align="right" height="110" alt="cartiflette" />

**Les fonds de carte officiels français, en une ligne de code.**

[![PyPI](https://img.shields.io/pypi/v/cartiflette?style=flat-square&color=B4540A&label=PyPI&logo=pypi&logoColor=white)](https://pypi.org/project/cartiflette/)
[![Téléchargements](https://img.shields.io/pepy/dt/cartiflette?style=flat-square&color=B4540A&label=t%C3%A9l%C3%A9chargements)](https://pepy.tech/projects/cartiflette)
[![Tests](https://img.shields.io/github/actions/workflow/status/InseeFrLab/cartiflette/check.yml?style=flat-square&label=tests&logo=github)](https://github.com/InseeFrLab/cartiflette/actions/workflows/check.yml)
[![Lint](https://img.shields.io/github/actions/workflow/status/InseeFrLab/cartiflette/lint.yml?style=flat-square&label=lint&logo=github)](https://github.com/InseeFrLab/cartiflette/actions/workflows/lint.yml)
[![Licence MIT](https://img.shields.io/badge/licence-MIT-B4540A?style=flat-square)](https://github.com/InseeFrLab/cartiflette/blob/main/LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.5-FFF000?style=flat-square&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![GeoParquet](https://img.shields.io/badge/GeoParquet-1.1-4B8BBE?style=flat-square)](https://geoparquet.org/)

Contours officiels de l'IGN (ADMIN EXPRESS COG CARTO) enrichis des métadonnées de
l'Insee : communes, arrondissements municipaux, départements, régions, bassins de
vie, zones d'emploi, unités urbaines, aires d'attraction des villes, avec ou sans
les DROM rapprochés de la métropole.

<img src="https://raw.githubusercontent.com/InseeFrLab/cartiflette/main/doc/images/demo.gif" alt="Exemples d'utilisation de cartiflette" width="820" />

## Installation

```bash
pip install cartiflette
```

Python 3.10 ou plus récent.

## Utilisation

```python
from cartiflette import carti_download

departements = carti_download(
    values="France",
    borders="DEPARTEMENT",
    filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
    year=2026,
)
departements.plot("POPULATION")
```

- `borders` : niveau des contours (`COMMUNE`, `COMMUNE_ARRONDISSEMENT`,
  `IRIS` à partir de 2025, `DEPARTEMENT`, `REGION`, `BASSIN_VIE`,
  `ZONE_EMPLOI`, `UNITE_URBAINE`, `AIRE_ATTRACTION_VILLES`).
- `filter_by` et `values` : zone couverte, par exemple `filter_by="REGION"` et
  `values=["11", "84"]`, ou `filter_by="FRANCE_ENTIERE"` et `values="France"`.
  Les codes de région sous 10 s'écrivent indifféremment `1`, `"1"` ou `"01"`.
- `simplification` : part des points retirés, `0` (contours complets), `50` ou
  `80` (le plus léger, par défaut). Pour 2022, publié seulement en GeoJSON, le
  défaut est `50`.
- `year` : millésime du Code officiel géographique.

Le résultat est un `GeoDataFrame` geopandas (EPSG:4326).

## Filtrer : `where`

`where` garde les polygones qui vérifient une expression SQL DuckDB, sur leurs
attributs ou leur géométrie (fonctions `ST_*` de l'extension spatiale, coordonnées
en longitude, latitude) :

```python
# Communes de plus de 2 000 habitants d'Occitanie
carti_download(
    values="76",
    borders="COMMUNE",
    filter_by="REGION",
    year=2026,
    where="POPULATION > 2000",
)

# Communes qui touchent une emprise (la Camargue)
carti_download(
    values="France",
    borders="COMMUNE",
    filter_by="FRANCE_ENTIERE",
    year=2026,
    where="ST_Intersects(geometry, ST_MakeEnvelope(4.1, 43.3, 4.9, 43.75))",
)

# Communes dont le centre est à moins de 30 km du Capitole de Toulouse
carti_download(
    values="France",
    borders="COMMUNE",
    filter_by="FRANCE_ENTIERE",
    year=2026,
    where="ST_Distance_Sphere(ST_Centroid(geometry), ST_Point(1.4442, 43.6047)) < 30000",
)
```

Sur le GeoParquet, le filtre est appliqué pendant la lecture : seules les parties
utiles du fichier sont téléchargées.

## Formats : GeoParquet d'abord

Chaque niveau est publié en GeoParquet dans un seul fichier, que DuckDB lit
partiellement : seules les parties correspondant aux valeurs demandées sont
téléchargées (environ 2 Mo pour les communes d'un département). Le client lit le
GeoParquet dès qu'il existe, même si `vectorfile_format="geojson"` est demandé (un
avertissement l'indique). Pour les millésimes publiés seulement en GeoJSON (2022),
le client le détecte et lit le GeoJSON.

Aucun fichier GeoJSON n'est publié à partir de 2025 : `force=True`, qui force la
lecture du GeoJSON, ne vaut que pour les millésimes qui en ont (2022). Pour obtenir
du GeoJSON des millésimes récents, par exemple depuis R ou JavaScript, utiliser
l'API : `https://cartiflette-api.lab.sspcloud.fr/v1/geojson`.

## Rester dans DuckDB

Toute la lecture est faite avec DuckDB. Avec `engine="duckdb"`, le résultat reste
une relation DuckDB, pour continuer en SQL sans passer par geopandas :

```python
communes = carti_download(
    values=["11", "84"],
    borders="COMMUNE",
    filter_by="REGION",
    year=2026,
    engine="duckdb",
)
communes.aggregate("INSEE_REG, sum(POPULATION)")
```

## Proxy

Déclarer la variable d'environnement `https_proxy` : elle est transmise à DuckDB.

```python
import os

os.environ["https_proxy"] = "http://mon-proxy:8080"
```

## En savoir plus

- [Documentation du client](https://inseefrlab.github.io/cartiflette/doc/client-python.html) :
  guide d'utilisation et [référence des fonctions](https://inseefrlab.github.io/cartiflette/doc/reference/).
- [Site de cartiflette](https://inseefrlab.github.io/cartiflette/) : exemples
  et cas d'usage, aussi en R et en JavaScript.
- [Dépôt GitHub](https://github.com/InseeFrLab/cartiflette) : pipeline de production
  et documentation technique.
- [Changelog](https://github.com/InseeFrLab/cartiflette/blob/main/python-package/cartiflette/CHANGELOG.md) :
  les changements de chaque version.

`cartiflette` est un projet collaboratif lancé par des agents de l'État dans le cadre
du [Programme 10 %](https://www.10pourcent.etalab.gouv.fr/). Pour contribuer, voir
[CONTRIBUTING.md](https://github.com/InseeFrLab/cartiflette/blob/main/CONTRIBUTING.md).
