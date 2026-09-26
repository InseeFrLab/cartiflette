# Notes sur le _pipeline_

Le _pipeline_ utilise la technologie `Argo`
et s'appuie sur l'infrastructure du `SSPCloud`
pour fonctionner.

Pour le lancer, dans un service ayant des droits admin
de Kubernetes :

```bash
argo submit argo-pipeline/pipeline.yaml \
  -p year=2025 \
  -p path=test/v0.2.0 \
  -p revision=main
```

## Structure du _pipeline_

1. `prepare` ([src/prepare.py](src/prepare.py)) : récupère sur la Géoplateforme de l'IGN
   l'édition France entière (WGS84) d'ADMIN EXPRESS COG CARTO de l'année (GeoParquet
   si disponible, GPKG sinon) et la table d'appartenance géographique (TAGC) de l'Insee.
   Produit `COMMUNE.geojson` et `COMMUNE_ARRONDISSEMENT.geojson` enrichis des zonages
   supra-communaux, sur le volume partagé entre les _pods_.
2. `list-jobs` et `list-parquet-jobs` ([src/crossproduct.py](src/crossproduct.py)) : listent
   les combinaisons à produire, 74 pour le GeoJSON et 32 pour le GeoParquet par millésime.
3. `split` ([src/split.py](src/split.py)) : GeoJSON. Pour chaque combinaison (niveau des
   polygones, niveau de découpage, simplification, projection), `mapshaper` agrège les
   communes, rapproche éventuellement les DROM, simplifie et découpe en un fichier par valeur.
4. `consolidate` ([src/consolidate.py](src/consolidate.py)) : GeoParquet. Pour chaque
   combinaison (niveau des polygones, disposition, simplification, projection), un seul
   fichier contenant tous les polygones du niveau, trié et découpé en petits groupes de
   lignes, que les clients filtrent à la lecture. La disposition (`layout`) vaut `FRANCE_ENTIERE` ou
   `FRANCE_ENTIERE_DROM_RAPPROCHES` (DROM rapprochés et zoom sur l'Île-de-France).

Seules les éditions 4-0 d'ADMIN EXPRESS (2025 et après) sont prises en charge.
Les millésimes antérieurs déjà publiés ne sont pas régénérés.

## Écriture sur S3

Le chemin d'écriture est le paramètre `path` du _workflow_ (`test/v<version>` par défaut, un dossier neuf par version pour ne pas écraser les tests précédents).
Le code refuse d'écrire sous `projet-cartiflette/production`, lu par les clients,
sauf si la variable d'environnement `CARTIFLETTE_ALLOW_PRODUCTION_WRITE` vaut
`i-know-what-i-am-doing`.

L'image `Docker` utilisée est `inseefrlab/cartiflette:v<version>`, construite à partir de
la version déclarée dans `pyproject.toml`.
