# cartiflette

`cartiflette` est un projet pour faciliter l'association de sources
géographiques en proposant des récupérations facilitées de coutours
de cartes officiels. 

## Installation

A l'heure actuelle, `cartiflette` est structuré sous
la forme d'un `package` :package: `Python` :snake:.
Ceci est amené à évoluer pour faciliter encore plus
la récupération de contours grâce à des API. 

Tout ceci est donc amené à bien évoluer, n'hésitez pas à 
revenir fréqumment sur cette page. 

```python
pip install git+https://github.com/inseefrlab/cartogether
```

Pour tester le package, vous pouvez tenter de récupérer
le contour des communes de la région Normandie:

```python
normandie = cartiflette.s3.download_vectorfile_url_all(
    values = "11",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

normandie.plot()
```

ou des régions Ile de France, Normandie et Bourgogne

```python
regions = cartiflette.s3.download_vectorfile_url_all(
    values = ["11","27","28"],
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

regions.plot()
```

## Plus de détails

- Pitch du projet ici: https://10pourcent.etalab.studio/projets/insee/
- Ateliers ici: https://github.com/etalab-ia/programme10pourcent/wiki/Ateliers-Faciliter-l%E2%80%99association-de-sources-de-donn%C3%A9es-g%C3%A9ographiques-issues-de-divers-producteurs-(INSEE,-IGN,-collectivit%C3%A9s-locales%E2%80%A6)#atelier2
