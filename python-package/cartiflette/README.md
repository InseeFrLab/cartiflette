# Cartiflette [![](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) ![](https://cdn.simpleicons.org/python/00ccff99?viewbox=auto&size=18)


`cartiflette` est un projet pour faciliter l’association de sources
géographiques en proposant des récupérations facilitées de coutours de
cartes officiels.

Une documentation interactive est disponible [ici](https://inseefrlab.github.io/cartiflette-website/index.html).

L'objectif de `cartiflette` est d'offrir des méthodes fiables, 
reproductibles et multi-langages pour récupérer des fonds de carte officiels de l'IGN
enrichis de métadonnées utiles pour la cartographie et la _data science_. 

La librairie python `cartiflette` ![](https://cdn.simpleicons.org/python/00ccff99?viewbox=auto&size=18) permet la récupération des fonds de carte de l'IGN.

## Installer la librairie python ![](https://cdn.simpleicons.org/python/00ccff99?viewbox=auto&size=18)
``` python
pip install cartiflette
```


## Exemples

Plus d'exemples sont disponibles dans la [documentation interactive](https://inseefrlab.github.io/cartiflette-website/index.html).

Exemple de récupération du fonds de carte des départements avec les DROM rapprochés de la France métropolitaine
``` python
from cartiflette import carti_download

data = carti_download(
    values = ["France"],
    crs = 4326,
    borders = "DEPARTEMENT",
    vectorfile_format="topojson",
    simplification=50,
    filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022
)
```

Si besoin de passer par un proxy, il faut déclarer http_proxy et https_proxy en variable d'environnement. Par exemple :
``` python
import os

from cartiflette import carti_download

os.environ["http_proxy"] = yourproxy
os.environ["https_proxy"] = yourproxy

data = carti_download(
    values = ["France"],
    crs = 4326,
    borders = "DEPARTEMENT",
    vectorfile_format="topojson",
    simplification=50,
    filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022
)
```

## Contexte

Le projet `cartiflette` est un projet collaboratif lancé par des agents de l'Etat dans le cadre d'un programme interministériel
nommé [Programme 10%](https://www.10pourcent.etalab.gouv.fr/).

__Vous désirez contribuer ?__ Plus d'information sont disponibles dans le fichier [CONTRIBUTING.md](https://github.com/InseeFrLab/cartiflette/blob/main/CONTRIBUTING.md)
