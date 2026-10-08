# Changelog du _pipeline_

_Pipeline_ de production (`cartiflette-pipeline`, non publié sur PyPI). Il
produit les fonds de carte publiés dans `projet-cartiflette/production`. Chaque
version est publiée comme image `inseefrlab/cartiflette:v<version>`. Le client
Python et l'API ont leur propre changelog
(`python-package/cartiflette/CHANGELOG.md`, `api/CHANGELOG.md`).

Les changements des fichiers publiés sont les plus importants : ce sont eux
que lisent les clients Python, R et JavaScript.

## 0.4.0 (à publier)

### Ajouté

- **Simplification 80** (part des points retirés), en plus de 0 et 50 :
  48 fichiers par millésime au lieu de 32. C'est le défaut du client 0.3.0, de
  l'API 0.1.4 et du site : ces fichiers doivent être publiés avant eux.
- **Niveau `IRIS`** (Contours IRIS de l'IGN, à partir de 2025, première
  édition France entière en WGS84) : 54 fichiers par millésime au lieu de 48.
  Champs historiques des Contours IRIS (`CODE_IRIS`, `NOM_IRIS`, `TYP_IRIS`,
  `NOM_COM`), plus `INSEE_COM`, `INSEE_COG` (arrondissement de Paris, Lyon et
  Marseille) et les champs de la TAGC, comme les communes ; pas de
  population. Filtrable par `COMMUNE` et `COMMUNE_ARRONDISSEMENT` en plus des
  filtres des communes ; trié par département puis code IRIS, pour que les
  IRIS d'une commune tiennent dans un groupe de lignes. Les IRIS de
  Saint-Pierre-et-Miquelon, Saint-Martin et Saint-Barthélemy sont exclus. En
  2026, ils suivent exactement les limites des communes d'ADMIN EXPRESS COG
  CARTO ; en 2025, quelques centaines de communes (surtout le Calvados et
  l'Eure-et-Loir) ont des limites un peu différentes (jusqu'à 8 % de la
  surface d'une commune). `list-jobs` ne liste que les niveaux préparés :
  pas d'IRIS avant 2025.

### Modifié

- **Une ligne par entité** dans la disposition
  `FRANCE_ENTIERE_DROM_RAPPROCHES`. Le zoom sur l'Île-de-France copiait Paris
  et la petite couronne : 105 départements, 19 régions, et des doublons pour
  les communes et les zonages de Paris. Une jointure avec des données faisait
  donc apparaître ces entités deux fois. La copie agrandie est maintenant une
  partie de plus du même multipolygone : 101 départements, 18 régions. Leur
  `bbox` couvre les deux emplacements.

### Corrigé

- `mapshaper -simplify ... keep-shapes` : sans cette option, la simplification
  80 donnait une géométrie nulle à 8 petits IRIS urbains de 2026. Les
  communes et les autres niveaux ne changent pas (fichiers identiques).
- Catalogue de la Géoplateforme lu sur `data.geopf.fr/telechargement` au lieu
  de `data.geopf.fr/chunk/telechargement`, qui ne liste pas Contours IRIS
  (mêmes éditions et fichiers pour ADMIN EXPRESS).
- `mapshaper -simplify` attend la part des points **gardés** : le _pipeline_
  lui passe `100 - simplification`. Les fichiers 0 et 50 ne changent pas
  (50 donne 50 dans les deux sens) ; 80 retire bien 80 % des points.

## 0.3.0 (2026-10-04)

Publié en production pour 2023 à 2026 (simplifications 0 et 50).

### Supprimé

- **Plus de GeoJSON** : le _pipeline_ ne produit que les GeoParquet consolidés,
  32 fichiers par millésime. L'étape `split` d'Argo et les fonctions
  `combinations`, `split_and_upload` et `mapshaper.split` sont retirées.
  L'API sert le GeoJSON à partir du GeoParquet. Les GeoJSON déjà publiés
  (2022) restent en place.

### Modifié

- Workflow Argo :
  - au plus 10 pods en même temps et 2 millésimes à la fois, arrêt au bout
    de 12 h ;
  - les pods des étapes réussies sont supprimés, et le workflow l'est
    lui-même après coup ;
  - les logs de chaque étape sont archivés dans
    `projet-cartiflette/argo-logs/`.
- `argo-pipeline/install-argo.sh` installe la CLI `argo` à la version du
  serveur ; `argo-pipeline/logs-app/` affiche les pods d'un workflow.

## 0.2.0 (2026-09-26)

Refonte complète.

### Ajouté

- **Sources** :
  - ADMIN EXPRESS COG CARTO « France entière » est récupéré sur la
    Géoplateforme de l'IGN : GeoParquet à partir de 2026, GPKG pour 2025,
    shapefile pour les éditions 3-x de 2021 à 2024 ;
  - les requêtes refusées pour dépassement de débit (429) sont retentées ;
  - les zonages viennent de la table d'appartenance géographique de
    l'Insee ; leurs millésimes (`BV2022`…) sont résolus à l'exécution.
- **GeoParquet consolidé** : un fichier par niveau, disposition (`layout`) et
  simplification.
  - Les lignes sont triées et regroupées en groupes de 2 048, avec une
    colonne `bbox` en _covering_ GeoParquet 1.1 : les clients ne lisent que
    les parties utiles.
  - La métadonnée `cartiflette:filter_columns` associe chaque `filter_by`
    à sa colonne.
- Contrôle de la cible d'écriture : toute écriture passe par `s3.upload`, qui
  refuse la production sans `CARTIFLETTE_ALLOW_PRODUCTION_WRITE=true`. La
  cible par défaut est `test/v<version>`.
- Workflow Argo :
  - une étape `check-target` vérifie la cible avant tout téléchargement ;
  - un sous-DAG est lancé par millésime de la liste `years`.

### Modifié

- Code fonctionnel (fonctions et dictionnaires), sans les anciennes classes
  `Dataset`, `Layer`, `MasterScraper`.
- Les noms de champs de l'édition 4-0 sont ramenés aux noms historiques
  (`INSEE_COM`, `NOM`, `POPULATION`…).
- Les fusions se font par code **et** territoire (`AREA`), ce qui corrige les
  DROM mal placés quand un code couvre plusieurs territoires (AAV2020 `000`).
- Saint-Pierre-et-Miquelon est exclu.
- DuckDB 1.5.5, mapshaper 0.6.59.

## Avant 0.2.0

L'ancien _pipeline_ (classes Python et mapshaper) a publié les GeoJSON de 2022,
un fichier par valeur, encore en production et lus par les clients 0.1.x.
