# Guide pour aider les développeurs du package <img height="18" width="18" src="https://cdn.simpleicons.org/python/00ccff99" /> `cartiflette`

Le _package_ <img height="18" width="18" src="https://cdn.simpleicons.org/python/00ccff99" /> `cartiflette` 
est une boite à outil qui répond principalement à deux objectifs :

- récupérer et restructurer les données diffusées par l'IGN pour produire des fonds de carte prêts à l'emploi
- interagir en écriture (mainteneurs du package) et en lecture (tous les utilisateurs) avec l'espace de stockage des fonds de carte proposés par `cartiflette`

## Structuration du package

Le package `cartiflette` est organisé en sous-packages thématiques:

- `cartiflette.utils`: une série de fonctions utilisées dans les autres sous-packages ou de fichiers de configuration utiles (comme le `sources.yaml`). Bien que certaines puissent être utiles à des utilisateurs du _package_, elles ont plutôt vocation à être des _internals_. 
- `cartiflette.download`: package pour communiquer avec le site de l'IGN. Les emplacements où aller chercher les fichiers sont dans le package `utils` et ce package se charge de télécharger, écrire dans un dossier temporaire et dézipper en local la source. 
Un système de _cache_ existe pour éviter de télécharger plusieurs fois le même fichier. 
- `cartiflette.s3`: le package qui gère l'interaction avec le système de stockage. Les fonctions sont à deux niveaux dans ce package
    + celles qui servent à écrire sur l'espace de stockage sont utilisées exclusivement par le _pipeline_ de production des fonds de carte `cartiflette`
    + celles qui servent à récupérer les fonds de carte `cartiflette` (depuis cet espace de stockage donc) ont vocation à être présentées
    aux utilisateurs finaux


## `cartiflette.download`

Les principales fonctions sont les suivantes:

- `create_url_adminexpress` (_internal_) : en fonction de paramètres de l'utilisateur (source, année...), récupération dans le fichier de config
de l'URL où aller chercher les données `IGN`
- `store_vectorfile_ign` : téléchargement des données IGN en fonction de paramètres de l'utilisateur (source, année...). Compter environ 500Mo pour les données Admin-Express et écriture dans un chemin standardisé
- `get_vectorfile_ign` : après `store_vectorfile_ign`, lecture sous forme de `DataFrame` `GeoPandas`
- `get_administrative_level_available_ign` : après `store_vectorfile_ign`, liste les niveaux administratifs disponibles
- `get_vectorfile_communes_arrondissement` : fait un `get_vectorfile_ign` sur deux niveaux administratifs, les arrondissements et les communes. Pour les trois villes à arrondissement, retire la commune et remplace par les arrondissements. 

Exemple : 

```python
from cartiflette.download import get_vectorfile_ign

get_vectorfile_ign(
    provider = "IGN",
    source = "EXPRESS-COG-TERRITOIRE",
    year = 2022,
    field = "metropole"
)
```

Bien que les fonctions de ce _package_
puissent être mises à disposition des utilisateurs finaux de `cartiflette`, elles
ont plutôt vocation à être utilisées lors de la production des fichiers de `cartiflette`.

## `cartiflette.s3`

Les fonctions d'écriture sur l'espace de stockage ayant vocation à être dans le _pipeline_ :

- `write_vectorfile_s3_all`
- `write_vectorfile_s3_custom_arrondissement`
- `production_cartiflette`

Les fonctions de récupération des fonds de carte ayant vocation à être mises à disposition
des utilisateurs finaux : 

- `download_vectorfile_s3_all`
- `download_vectorfile_url_all`
- `list_produced_cartiflette`

Galerie d'exemples :

- Des exemples à venir dans le cours de l'ENSAE _"Python pour la data science"_
- Notebook `Observable` qui génère les bouts de code utiles ayant vocation à être dans la documentation: https://observablehq.com/@linogaliana/cartiflette-demo
