"""
Handling spatial data with mapshaper behind the stage
"""
from .mapshaperize import mapshaperize_split, mapshaperize_split_merge
from .mapshaper_convert_mercator import mapshaper_convert_mercator
from .mapshaper_closer import mapshaper_bring_closer
from .mapshaper_wrangling import mapshaper_enrich, mapshaper_split

__all__ = [
    "mapshaperize_split",
    "mapshaperize_split_merge",
    "mapshaper_convert_mercator",
    "mapshaper_bring_closer",
    "mapshaper_enrich",
    "mapshaper_split",
]
