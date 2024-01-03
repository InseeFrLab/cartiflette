"""
Handling spatial data with mapshaper behind the stage
"""
from .mapshaperize import mapshaperize_split, mapshaperize_split_merge
from .mapshaper_convert_mercator import mapshaper_convert_mercator

__all__ = [
    "mapshaperize_split", "mapshaperize_split_merge", "mapshaper_convert_mercator"
]