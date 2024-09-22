"""
Handling spatial data with mapshaper behind the stage
"""

from .mapshaper_split import mapshaper_split
from .mapshaper_convert_reproject import mapshaper_convert_reproject
from .mapshaper_closer import mapshaper_bring_closer
from .mapshaper_enrich import mapshaper_enrich
from .mapshaper_dissolve import mapshaper_dissolve
from .mapshaper_concat import mapshaper_concat
from .mapshaper_remove_cities_with_districts import (
    mapshaper_remove_cities_with_districts,
)
from .mapshaper_process_communal_districts import (
    mapshaper_process_communal_districts,
)
from .mapshaper_combine_districts_and_cities import (
    mapshaper_combine_districts_and_cities,
)
from .mapshaper_simplify import mapshaper_simplify
from .mapshaper_add_field import mapshaper_add_field


__all__ = [
    "mapshaper_convert_reproject",
    "mapshaper_bring_closer",
    "mapshaper_enrich",
    "mapshaper_split",
    "mapshaper_dissolve",
    "mapshaper_concat",
    "mapshaper_remove_cities_with_districts",
    "mapshaper_process_communal_districts",
    "mapshaper_combine_districts_and_cities",
    "mapshaper_simplify",
    "mapshaper_add_field",
]
