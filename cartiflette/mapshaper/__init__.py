"""
Handling spatial data with mapshaper behind the stage
"""
from .mapshaper_split import mapshaper_split
from .mapshaper_convert_mercator import mapshaper_convert_mercator
from .mapshaper_closer import mapshaper_bring_closer
from .mapshaper_enrich import mapshaper_enrich
from .mapshaper_dissolve import mapshaper_dissolve
from .mapshaper_concat import mapshaper_concat
from .mapshaper_remove_cities_with_districts import (
    mapshaper_remove_cities_with_districts,
)
from .mapshaper_preprocess_communal_districts import (
    mapshaper_preprocess_communal_districts,
)
from .mapshaper_combine_districts_and_cities import (
    mapshaper_combine_districts_and_cities,
)


__all__ = [
    "mapshaper_convert_mercator",
    "mapshaper_bring_closer",
    "mapshaper_enrich",
    "mapshaper_split",
    "mapshaper_dissolve",
    "mapshaper_concat",
    "mapshaper_remove_cities_with_districts",
    "mapshaper_preprocess_communal_districts",
    "mapshaper_combine_districts_and_cities",
]
