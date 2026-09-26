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
2. `list-jobs` ([src/crossproduct.py](src/crossproduct.py)) : liste les combinaisons
   (niveau des polygones, niveau de découpage, simplification, projection).
3. `split` ([src/split.py](src/split.py)) : pour chaque combinaison, `mapshaper`
   agrège les communes, rapproche éventuellement les DROM, simplifie et découpe
   en un fichier par valeur. Chaque fichier est écrit sur S3 en GeoJSON et en
   GeoParquet (conversion par DuckDB).

Seules les éditions 4-0 d'ADMIN EXPRESS (2025 et après) sont prises en charge.
Les millésimes antérieurs déjà publiés ne sont pas régénérés.

## Écriture sur S3

Le chemin d'écriture est le paramètre `path` du _workflow_ (`test/v<version>` par défaut, un dossier neuf par version pour ne pas écraser les tests précédents).
Le code refuse d'écrire sous `projet-cartiflette/production`, lu par les clients,
sauf si la variable d'environnement `CARTIFLETTE_ALLOW_PRODUCTION_WRITE` vaut
`i-know-what-i-am-doing`.

L'image `Docker` utilisée est `inseefrlab/cartiflette:v<version>`, construite à partir de
la version déclarée dans `pyproject.toml`.
