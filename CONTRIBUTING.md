# Guide pour aider les développeurs du package <img height="18" width="18" src="https://cdn.simpleicons.org/python/00ccff99" /> `cartiflette`

Le dépôt contient deux choses distinctes :

- le _pipeline_ de production des fonds de carte (dossier `cartiflette/`, orchestré par `argo-pipeline/`) :
  il récupère les données de l'IGN et de l'Insee, les restructure avec `mapshaper` et écrit
  des fichiers GeoJSON et GeoParquet sur l'espace de stockage S3 ;
- le client `cartiflette` publié sur PyPI (dossier `python-package/cartiflette/`), qui lit
  ces fichiers.

Le seul contrat entre les deux est le chemin des fichiers sur S3, construit par
`cartiflette/paths.py` d'un côté et `python-package/cartiflette/cartiflette/utils.py` de l'autre.
Ces deux fonctions doivent rester identiques.

Pour reprendre le projet (architecture, lancement du _pipeline_, vérifications), voir la
site de documentation Quarto dans [doc/](doc/index.qmd) (`cd doc && quarto preview`).

## Structure du _pipeline_

Le code est écrit sous forme de fonctions, chaque module correspondant à une étape :

- `cartiflette.ign` : récupération d'ADMIN EXPRESS COG CARTO sur la Géoplateforme (catalogue Atom
  du service de téléchargement), en GeoParquet ou GPKG ;
- `cartiflette.insee` : récupération de la table d'appartenance géographique des communes (TAGC) ;
- `cartiflette.prepare` : normalisation des données (noms de champs historiques, enrichissement par
  la TAGC, communes avec arrondissements municipaux) avec DuckDB ;
- `cartiflette.mapshaper` : agrégation, rapprochement des DROM, simplification et découpage avec `mapshaper` ;
- `cartiflette.pipeline` : enchaînement des étapes, conversion GeoParquet et écriture sur S3 ;
- `cartiflette.s3` : écriture sur S3, qui refuse d'écrire dans le dossier `production` lu par les clients ;
- `cartiflette.config` : emplacements de lecture et d'écriture.

## Tests

```shell
uv run pytest tests                                   # pipeline (mapshaper requis pour certains tests)
cd python-package/cartiflette && uv run pytest tests  # client
```

Les tests n'écrivent jamais sur S3.
