from datetime import date
from functools import reduce, lru_cache
import logging
import os
import typing
from warnings import warn

from requests_cache import CachedSession
import geopandas as gpd
import pandas as pd

from cartiflette.constants import (
    DIR_CACHE,
    CACHE_NAME,
    BUCKET,
    PATH_WITHIN_BUCKET,
    CATALOG,
)

from cartiflette.config import _config
from cartiflette.utils import (
    create_path_bucket,
    standardize_inputs,
    flatten_dict,
)

logger = logging.getLogger(__name__)

session = CachedSession()


class CartifletteSession(CachedSession):

    CACHE_NAME = os.path.join(DIR_CACHE, CACHE_NAME)

    def __init__(
        self, expire_after: int = _config["DEFAULT_EXPIRE_AFTER"], **kwargs
    ):
        super().__init__(
            cache_name=self.CACHE_NAME,
            expire_after=expire_after,
            **kwargs,
        )

        for protocol in ["http", "https"]:
            try:
                proxy = {protocol: os.environ[f"{protocol}_proxy"]}
                self.proxies.update(proxy)
            except KeyError:
                continue

    def download_cartiflette_single(
        self,
        provider: str = "Cartiflette",
        dataset_family: str = "production",
        source: str = "EXPRESS-COG-TERRITOIRE",
        vectorfile_format: str = "geojson",
        borders: str = "COMMUNE",
        filter_by: str = "region",
        territory: str = "metropole",
        year: typing.Union[str, int, float] = None,
        value: typing.Union[str, int, float] = "28",
        crs: typing.Union[list, str, int, float] = 2154,
        simplification: typing.Union[str, int, float] = None,
        filename: str = "raw",
    ) -> gpd.GeoDataFrame:
        """
        Download a single geodataset from Cartiflette

        Parameters
        ----------
        provider : str, optional
            Deprecated. The default is "Cartiflette".
        dataset_family : str, optional
            Deprecated. The default is "production".
        source : str, optional
            DESCRIPTION. The default is "EXPRESS-COG-TERRITOIRE".
        vectorfile_format : str, optional
            DESCRIPTION. The default is "geojson".
        borders : str, optional
            DESCRIPTION. The default is "COMMUNE".
        filter_by : str, optional
            DESCRIPTION. The default is "region".
        territory : str, optional
            DESCRIPTION. The default is "metropole".
        year : typing.Union[str, int, float], optional
            DESCRIPTION. The default is None.
        value : typing.Union[str, int, float], optional
            DESCRIPTION. The default is "28".
        crs : typing.Union[list, str, int, float], optional
            DESCRIPTION. The default is 2154.
        simplification : typing.Union[str, int, float], optional
            DESCRIPTION. The default is None.
        filename : str, optional
            DESCRIPTION. The default is "raw".
         : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        if provider:
            warn(
                "provider is deprecated and will be removed in a future "
                "version. You can safely drop this argument.",
                DeprecationWarning,
                stacklevel=2,
            )

        if provider:
            warn(
                "dataset_family is deprecated and will be removed in a future "
                "version. You can safely drop this argument.",
                DeprecationWarning,
                stacklevel=2,
            )

        if borders == "COMMUNE_ARRONDISSEMENT":
            warn(
                "'COMMUNE_ARRONDISSESMENT' is deprecated for borders and will "
                "be removed in a future version. Please use "
                "'ARRONDISSEMENT_MUNICIPAL' instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        if not year:
            year = str(date.today().year)

        _corresp_filter_by_columns, format_read, _driver = standardize_inputs(
            vectorfile_format
        )

        url = create_path_bucket(
            {
                "bucket": BUCKET,
                "path_within_bucket": PATH_WITHIN_BUCKET,
                "vectorfile_format": format_read,
                "territory": territory,
                "borders": borders,
                "filter_by": filter_by,
                "year": year,
                "value": value,
                "crs": crs,
                "provider": "Cartiflette",
                "dataset_family": "production",
                "source": source,
                "simplification": simplification,
                "filename": filename,
            }
        )

        url = f"https://minio.lab.sspcloud.fr/{url}"

        try:
            r = self.get(url)
            gdf = gpd.read_file(r.content)
        except Exception as e:
            logger.error(
                "There was an error while reading the file from the URL: %s",
                url,
            )
            logger.error("Error message: %s", str(e))
            return gpd.GeoDataFrame()
        else:
            return gdf

    def get_catalog(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve and load cartiflette's current datasets' catalog (as a
        dataframe), filtered on any of the following columns:
        [
            'source',
            'year',
            'administrative_level',
            'crs',
            'filter_by',
            'value',
            'vectorfile_format',
            'territory',
            'simplification'
        ]

        Each row corresponds to an available DataFrame.

        Parameters
        ----------
        kwargs: dict
            pairs of column/filter values

        Returns
        -------
        df : pd.DataFrame
            Filtered catalog as DataFrame

        Example
        -------
        >>> kwargs = {"territory": "france", "source": "CONTOUR-IRIS"}
        >>> with CartifletteSession() as carti_session:
            return carti_session.get_catalog(**kwargs)

                    source  year  ... territory simplification
        0     CONTOUR-IRIS  2023  ...    france             40
        1     CONTOUR-IRIS  2023  ...    france             40
        2     CONTOUR-IRIS  2023  ...    france             40
        3     CONTOUR-IRIS  2023  ...    france             40
        4     CONTOUR-IRIS  2023  ...    france             40
                   ...   ...  ...       ...            ...
        5745  CONTOUR-IRIS  2023  ...    france             40
        5746  CONTOUR-IRIS  2023  ...    france             40
        5747  CONTOUR-IRIS  2023  ...    france             40
        5748  CONTOUR-IRIS  2023  ...    france             40
        5749  CONTOUR-IRIS  2023  ...    france             40

        [5750 rows x 9 columns]

        """
        df = self._get_full_catalog()
        if kwargs:
            mask = reduce(
                lambda x, y: x & y, [df[k] == v for k, v in kwargs.items()]
            )
            df = df[mask].copy()
        return df

    def _get_full_catalog(self) -> pd.DataFrame:
        """
        Retrieve and load cartiflette's current datasets' catalog (as a
        dataframe).

        Inventory columns are [
             'source',
             'year',
             'administrative_level',
             'crs',
             'filter_by',
             'value',
             'vectorfile_format',
             'territory',
             'simplification'
             ]

        Each row corresponds to an available DataFrame.

        Returns
        -------
        df : pd.DataFrame
            Inventory DataFrame

        """

        url = CATALOG
        try:
            r = self.get(url)
            d = r.json()
        except Exception as e:
            logger.error(
                "There was an error while reading the file from the URL: %s",
                url,
            )
            logger.error("Error message: %s", str(e))
            return

        d = flatten_dict(d)

        index = pd.MultiIndex.from_tuples(d.keys())
        df = pd.DataFrame(
            list(d.values()), index=index, columns=["simplification"]
        )
        index.names = [
            "source",
            "year",
            "administrative_level",
            "crs",
            "filter_by",
            "value",
            "vectorfile_format",
            "territory",
        ]

        df = df.reset_index(drop=False)

        return df

    def get_dataset(
        self,
        values: typing.List[typing.Union[str, int, float]],
        borders: str = "COMMUNE",
        filter_by: str = "region",
        territory: str = "metropole",
        vectorfile_format: str = "geojson",
        year: typing.Union[str, int, float] = None,
        crs: typing.Union[list, str, int, float] = 2154,
        simplification: typing.Union[str, int, float] = None,
        provider: str = "Cartiflette",
        dataset_family: str = "production",
        source: str = "EXPRESS-COG-TERRITOIRE",
        filename: str = "raw",
        return_as_json: bool = False,
    ) -> typing.Union[gpd.GeoDataFrame, str]:
        # TODO : fix docstring
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
        - provider, dataset_family, source:
            Other parameters required for accessing the Cartiflette API.

        - return_as_json (bool, optional):
            If True, the function returns a JSON string representation of the
            aggregated GeoDataFrame. If False, it returns a GeoDataFrame. Default
            is False.

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
            gdf_single = self.download_cartiflette_single(
                value=value,
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


def carti_download(
    values: typing.List[typing.Union[str, int, float]],
    *args,
    borders: str = "COMMUNE",
    filter_by: str = "region",
    territory: str = "metropole",
    vectorfile_format: str = "geojson",
    year: typing.Union[str, int, float] = None,
    crs: typing.Union[list, str, int, float] = 2154,
    simplification: typing.Union[str, int, float] = None,
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
    - provider, dataset_family, source:
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
            provider=provider,
            dataset_family=dataset_family,
            source=source,
            filename=filename,
            return_as_json=return_as_json,
            **kwargs,
        )


@lru_cache(maxsize=128)
def get_catalog(**kwargs) -> pd.DataFrame:
    """
    Retrieve Cartiflette's catalog. If kwargs are specified, will filter that
    catalog according to the pairs of column/values given.

    This function is cached.

    Parameters
    ----------
    kwargs :
        Pairs of keys/values from the catalog, optional.

    Returns
    -------
    pd.DataFrame
        Catalog of available datasets.

    Example
    -------
    >>> get_catalog(territory="france", source="CONTOUR-IRIS")

                source  year  ... territory simplification
    0     CONTOUR-IRIS  2023  ...    france             40
    1     CONTOUR-IRIS  2023  ...    france             40
    2     CONTOUR-IRIS  2023  ...    france             40
    3     CONTOUR-IRIS  2023  ...    france             40
    4     CONTOUR-IRIS  2023  ...    france             40
               ...   ...  ...       ...            ...
    5745  CONTOUR-IRIS  2023  ...    france             40
    5746  CONTOUR-IRIS  2023  ...    france             40
    5747  CONTOUR-IRIS  2023  ...    france             40
    5748  CONTOUR-IRIS  2023  ...    france             40
    5749  CONTOUR-IRIS  2023  ...    france             40

    [5750 rows x 9 columns]

    """
    with CartifletteSession() as carti_session:
        return carti_session.get_catalog(**kwargs)
