"""
Production pipeline of the cartiflette files: retrieves the IGN and Insee
sources, processes them with mapshaper and writes GeoJSON and GeoParquet files
to S3. To read those files, use the `cartiflette` client (python-package/).
"""

__version__ = "0.3.0"
