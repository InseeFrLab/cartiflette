from requests_cache import CachedSession
import os
import typing
import geopandas as gpd
from datetime import date

from cartiflette.constants import DIR_CACHE, CACHE_NAME, BUCKET, PATH_WITHIN_BUCKET
from cartiflette.config import _config
from cartiflette.utils import download_cartiflette_single

session = CachedSession()


class CartifletteSession(CachedSession):

    CACHE_NAME = os.path.join(DIR_CACHE, CACHE_NAME)

    def __init__(
        self,
        expire_after: int = _config["DEFAULT_EXPIRE_AFTER"],
        **kwargs,
    ):
        super().__init__(
            cache_name=self.CACHE_NAME,
            expire_after=expire_after,
            **kwargs,
        )

    def get_dataset(
        self,
        values: typing.List[typing.Union[str, int, float]],
        *args,
        borders: str = "COMMUNE",
        filter_by: str = "region",
        territory: str = "metropole",
        vectorfile_format: str = "geojson",
        year: typing.Union[str, int, float] = None,
        crs: typing.Union[list, str, int, float] = 2154,
        simplification: typing.Union[str, int, float] = None,
        bucket: str = BUCKET,
        path_within_bucket: str = PATH_WITHIN_BUCKET,
        provider: str = "IGN",
        dataset_family: str = "ADMINEXPRESS",
        source: str = "EXPRESS-COG-TERRITOIRE",
        filename: str = "raw",
        return_as_json: bool = False,
        **kwargs,
    ) -> typing.Union[gpd.GeoDataFrame, str]:
        """
        Downloads and aggregates official geographic datasets using the Cartiflette API
        for a set of specified values.
        Optionally returns the data as a JSON string.

        This function is useful for downloading and concatenating data related to different regions,
        communes, etc., into a single GeoDataFrame or JSON string.

        Parameters:
        - values (List[Union[str, int, float]]):
            A list of values to filter data by the filter_by parameter.
        - borders (str, optional):
            The type of borders (default is "COMMUNE").
        - filter_by (str, optional):
            The parameter to filter by (default is "region").
        - territory (str, optional):
            The territory (default is "metropole").
        - vectorfile_format (str, optional):
            The file format for vector files (default is "geojson").
        - year (Union[str, int, float], optional):
            The year of the dataset. Defaults to the current year if not provided.
        - crs (Union[list, str, int, float], optional):
            The coordinate reference system (default is 2154).
        - simplification (Union[str, int, float], optional):
            The simplification parameter (default is None).
        - bucket, path_within_bucket, provider, dataset_family, source:
            Other parameters required for accessing the Cartiflette API.

        - return_as_json (bool, optional):
            If True, the function returns a JSON string representation of the aggregated GeoDataFrame.
            If False, it returns a GeoDataFrame. Default is False.

        Returns:
        - Union[gpd.GeoDataFrame, str]:
            A GeoDataFrame containing concatenated data from the
                specified parameters if return_as_json is False.
            A JSON string representation of the GeoDataFrame
                if return_as_json is True.
        """

        # Initialize an empty list to store individual GeoDataFrames
        gdf_list = []

        # Set the year to the current year if not provided
        if not year:
            year = str(date.today().year)

        if isinstance(values, (str, int)):
            values = [values]

        # Iterate over values
        for value in values:
            gdf_single = download_cartiflette_single(
                value=value,
                bucket=bucket,
                path_within_bucket=path_within_bucket,
                provider=provider,
                dataset_family=dataset_family,
                source=source,
                vectorfile_format=vectorfile_format,
                borders=borders,
                filter_by=filter_by,
                territory=territory,
                year=year,
                crs=crs,
                simplification=simplification,
                filename=filename,
            )
            gdf_list.append(gdf_single)

        # Concatenate the list of GeoDataFrames into a single GeoDataFrame
        concatenated_gdf = gpd.pd.concat(gdf_list, ignore_index=True)

        if return_as_json is True:
            return concatenated_gdf.to_json()

        return concatenated_gdf


def carti_dataset(
    values: typing.List[typing.Union[str, int, float]],
    *args,
    borders: str = "COMMUNE",
    filter_by: str = "region",
    territory: str = "metropole",
    vectorfile_format: str = "geojson",
    year: typing.Union[str, int, float] = None,
    crs: typing.Union[list, str, int, float] = 2154,
    simplification: typing.Union[str, int, float] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-TERRITOIRE",
    filename: str = "raw",
    return_as_json: bool = False,
    **kwargs,
) -> typing.Union[gpd.GeoDataFrame, str]:
    """
    Calls CartifletteSession.get_dataset
    Downloads and aggregates official geographic datasets using the Cartiflette API
    for a set of specified values.
    Optionally returns the data as a JSON string.

    This function is useful for downloading and concatenating data related to different regions,
    communes, etc., into a single GeoDataFrame or JSON string.

    Parameters:
    - values (List[Union[str, int, float]]):
        A list of values to filter data by the filter_by parameter.
    - borders (str, optional):
        The type of borders (default is "COMMUNE").
    - filter_by (str, optional):
        The parameter to filter by (default is "region").
    - territory (str, optional):
        The territory (default is "metropole").
    - vectorfile_format (str, optional):
        The file format for vector files (default is "geojson").
    - year (Union[str, int, float], optional):
        The year of the dataset. Defaults to the current year if not provided.
    - crs (Union[list, str, int, float], optional):
        The coordinate reference system (default is 2154).
    - simplification (Union[str, int, float], optional):
        The simplification parameter (default is None).
    - bucket, path_within_bucket, provider, dataset_family, source:
        Other parameters required for accessing the Cartiflette API.

    - return_as_json (bool, optional):
        If True, the function returns a JSON string representation of the aggregated GeoDataFrame.
        If False, it returns a GeoDataFrame. Default is False.

    Returns:
    - Union[gpd.GeoDataFrame, str]:
        A GeoDataFrame containing concatenated data from the
            specified parameters if return_as_json is False.
        A JSON string representation of the GeoDataFrame
            if return_as_json is True.
    """

    with CartifletteSession() as carti_session:
        return carti_session.get_dataset(
            values=values,
            *args,
            borders=borders,
            filter_by=filter_by,
            territory=territory,
            vectorfile_format=vectorfile_format,
            year=year,
            crs=crs,
            simplification=simplification,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider=provider,
            dataset_family=dataset_family,
            source=source,
            filename=filename,
            return_as_json=return_as_json,
            **kwargs,
        )
