# -*- coding: utf-8 -*-

from contextlib import ExitStack
from copy import deepcopy
from glob import glob
from itertools import product
import logging
import os
import re
import shutil
import tempfile
from typing import List

try:
    from typing import Self
except ImportError:
    # python < 3.11
    Self = "S3GeoDataset"

import geopandas as gpd
from pebble import ThreadPool
from s3fs import S3FileSystem

from .dataset import S3Dataset
from cartiflette.mapshaper import (
    mapshaper_convert_reproject,
    mapshaper_enrich,
    mapshaper_bring_closer,
    mapshaper_split,
    mapshaper_dissolve,
    mapshaper_concat,
    mapshaper_remove_cities_with_districts,
    mapshaper_process_communal_districts,
    mapshaper_combine_districts_and_cities,
    mapshaper_simplify,
    mapshaper_add_field,
    mapshaper_capture_cities_from_ultramarine_territories,
)
from cartiflette.utils import ConfigDict
from cartiflette.config import (
    FS,
    THREADS_DOWNLOAD,
    INTERMEDIATE_FORMAT,
    MAPSHAPER_QUIET,
)
from cartiflette.pipeline_constants import PIPELINE_CRS, PIPELINE_FORMATS
from cartiflette.utils.dict_correspondance import (
    create_format_driver,
    create_format_standardized,
)

logger = logging.getLogger(__name__)


class S3GeoDataset(S3Dataset):
    """
    Base class representing a geodataset stored on the S3

    An instance can either be an existing file loaded from the S3 or a new
    geodataset in the process of creation. In that case, a warning will be
    displayed at creation to alert that the file is not present on the S3
    (yet).
    """

    def __str__(self):
        return f"<cartiflette.s3.dataset.S3GeoDataset({self.config})>"

    def __copy__(self):
        """
        Copy a S3GeoDataset. If the original S3GeoDataset has already a
        local_dir attribute, this will create a new tempdir inside it.
        Note that this new tempdir will be removed at the primary S3GeoDataset
        object's __exit__ method execution.

        Returns
        -------
        new : S3GeoDataset
            Copied S3GeoDataset.

        """

        if os.path.exists(os.path.join(self.local_dir, self.main_filename)):
            # file is already on local disk -> create a new tempdir that should
            # be cleaned on __exit__method anyway
            new_tempdir = tempfile.mkdtemp()
            target_name = self.main_filename.rsplit(".", maxsplit=1)[0]
            for file in glob(os.path.join(self.local_dir, f"{target_name}.*")):
                shutil.copy(file, new_tempdir)

            new = S3GeoDataset(
                self.fs,
                self.filename,
                build_from_local=os.path.join(
                    self.local_dir, self.main_filename
                ),
                **deepcopy(self.config),
            )
            new.local_dir = new_tempdir

        else:
            new = S3GeoDataset(
                self.fs,
                self.filename,
                self.build_from_local,
                **deepcopy(self.config),
            )

        new.main_filename = self.main_filename

        return new

    def to_format(self, format_output: str, epsg: int):
        if format_output == INTERMEDIATE_FORMAT and epsg == 4326:
            return self

        if format_output in {
            "shapefile",
            "geojson",
            "topojson",
            "json",
            "dbf",
            "csv",
            "tsv",
            "svg",
        }:
            self.reproject(epsg=epsg, format_output=format_output)
        else:
            getattr(self, f"to_{format_output}")(epsg)
        return self

    def to_gpkg(self, epsg: int):
        """
        Replace the current main_file by a geopackage format (not handled by
        mapshaper, needs geopandas)
        """

        init_level = logging.getLogger("pyogrio").level
        if MAPSHAPER_QUIET:
            logging.getLogger("pyogrio._io").setLevel(logging.CRITICAL)

        try:
            path = os.path.join(self.local_dir, self.main_filename)
            path = path.rsplit(".", maxsplit=1)[0] + ".gpkg"
            gdf = self.to_frame()
            if epsg != 4326:
                gdf = gdf.to_crs(epsg)
            gdf.to_file(path, driver="GPKG")
            self._substitute_main_file(path)
            self.config["vectorfile_format"] = "gpkg"
            self.config["crs"] = epsg
            self.update_s3_path_evaluation()
        except Exception:
            raise
        finally:
            logging.getLogger("pyogrio").setLevel(init_level)

    # def to_shapefile(self):
    #     """
    #     TODO Quick and dirty hack, to be removed to handle native mapshaper
    #     output
    #     Replace the current main_file by a shapefile format (using geopandas)
    #     """
    #     path = os.path.join(self.local_dir, self.main_filename)
    #     path = path.rsplit(".", maxsplit=1)[0] + ".shp"
    #     self.to_frame().to_file(path)
    #     self._substitute_main_file(path)
    #     self.config["vectorfile_format"] = "shp"
    #     self.update_s3_path_evaluation()

    def to_frame(self, **kwargs) -> gpd.GeoDataFrame:
        "Read the geodataset from local file"
        return gpd.read_file(
            os.path.join(self.local_dir, self.main_filename), **kwargs
        )

    def _get_columns(self, **kwargs):
        "Get the columns of the dataset"
        df = self.to_frame(**kwargs, rows=5)
        return df.columns.tolist()

    def copy(self):
        """
        Create a deepcopy of the S3GeoDataset (with a copy of initial file on
        a new local dir if the initial object has a local file)
        """
        return self.__copy__()

    def _substitute_main_file(self, new_file: str):
        "Set a new file as reference for the S3GeoDataset from local disk"
        if not os.path.dirname(new_file) == self.local_dir:
            raise ValueError(
                f"cannot substitute main_file with {new_file=} and "
                f"{self.local_dir=} : directories are not identical"
            )

        if os.path.basename(new_file) == self.main_filename:
            return

        os.unlink(f"{self.local_dir}/{self.main_filename}")
        self.main_filename = os.path.basename(new_file)

    def reproject(
        self,
        epsg: int = 4326,
        format_output: str = "geojson",
        quiet: bool = MAPSHAPER_QUIET,
    ):
        "project to a given EPSG using mapshaper"
        input_file = f"{self.local_dir}/{self.main_filename}"

        new_file = mapshaper_convert_reproject(
            input_file=input_file,
            epsg=epsg,
            output_dir=self.local_dir,
            output_name=self.main_filename.rsplit(".", maxsplit=1)[0],
            output_format=format_output,
            filter_by=self.config["territory"],
            quiet=quiet,
        )
        self._substitute_main_file(new_file)
        self.config["crs"] = epsg
        self.config["vectorfile_format"] = format_output
        self.update_s3_path_evaluation()
        return new_file

    def add_field(
        self,
        label: str,
        value: str,
        format_output: str = "geojson",
        quiet: bool = MAPSHAPER_QUIET,
    ):
        "add a static/dynamic field using mapshaper"
        input_geodata = f"{self.local_dir}/{self.main_filename}"
        output = mapshaper_add_field(
            input_file=input_geodata,
            label=label,
            value=value,
            output_dir=self.local_dir,
            output_name=self.main_filename.rsplit(".", maxsplit=1)[0],
            output_format=format_output,
            quiet=quiet,
        )
        self._substitute_main_file(output)

    def enrich(
        self,
        metadata_file: S3Dataset,
        keys: list,
        dtype: dict,
        drop: list,
        rename: dict,
        format_output: str = "geojson",
        quiet: bool = MAPSHAPER_QUIET,
    ):
        "enrich with metadata using mapshaper"
        input_metadata = (
            f"{metadata_file.local_dir}/{metadata_file.main_filename}"
        )
        input_geodata = f"{self.local_dir}/{self.main_filename}"
        output = mapshaper_enrich(
            input_geodata_file=input_geodata,
            input_metadata_file=input_metadata,
            keys=keys,
            dtype=dtype,
            drop=drop,
            rename=rename,
            output_dir=self.local_dir,
            output_name=self.main_filename.rsplit(".", maxsplit=1)[0],
            output_format=format_output,
            quiet=quiet,
        )
        self._substitute_main_file(output)

    def simplify(
        self,
        format_output: str,
        simplification: int = 0,
        quiet: bool = MAPSHAPER_QUIET,
    ):
        "simplify the geometries"
        simplification = simplification if simplification else 0
        if simplification != 0:
            option_simplify = (
                f"-simplify {simplification}% interval=.5 -clean "
            )
        else:
            option_simplify = ""

        input_geodata = f"{self.local_dir}/{self.main_filename}"
        output = mapshaper_simplify(
            input_geodata,
            option_simplify=option_simplify,
            output_dir=self.local_dir,
            output_name=self.main_filename.rsplit(".", maxsplit=1)[0],
            output_format=format_output,
            quiet=quiet,
        )

        # update path on S3
        self.config["simplification"] = simplification
        self._substitute_main_file(output)
        self.update_s3_path_evaluation()

        if format_output.lower() == "topojson":
            # cannot fix geometries with geopandas anyway
            return

        format_standardized = create_format_standardized()
        gpd_driver = create_format_driver()
        format_write = format_standardized[format_output.lower()]
        driver = gpd_driver[format_write]

        # Ensure geometries' validity
        gdf = gpd.read_file(output)
        if not gdf["geometry"].is_valid.all():
            gdf["geometry"] = gdf["geometry"].buffer(0)
            gdf.to_file(output, driver=driver)

    def dissolve(
        self,
        by: List[str],
        copy_fields: List[str] = None,
        calc: List[str] = None,
        format_output: str = "geojson",
        quiet: bool = MAPSHAPER_QUIET,
    ):
        """
        Dissolve geometries and rename local file using mapshaper.

        Dissolve geometries on field `bv`, keeping fields `copy_fields`. Other
        fields should be computaded using javascript functions with `calc`
        argument. The original file will be overwritten, then renamed to
        {by}.{formate_intermediate}. self.main_filename will be updated.


        Parameters
        ----------
        by : List[str]
            Fields used to dissolve
        calc : Listr[str], optional
            Fields on which computed should be operated, describing valid js
            functions. For instance ["POPULATION=sum(POPULATION)"]. The default
            is None.
        copy_fields : List[str], optional
            Copies values from the first feature in each group of dissolved
            features. The default is None.
        format_output : str, optional
            Output format. The default is geojson
        quiet : bool, optional
            If True, inhibits console messages. The default is MAPSHAPER_QUIET.

        Returns
        -------
        None.

        """
        init = f"{self.local_dir}/{self.main_filename}"
        out = mapshaper_dissolve(
            input_file=init,
            by=by,
            copy_fields=copy_fields,
            calc=calc,
            output_dir=self.local_dir,
            output_name="_".join(by),
            output_format=format_output,
            quiet=quiet,
        )
        self._substitute_main_file(out)

    def bring_drom_closer(
        self,
        level_agreg: str = "DEPARTEMENT",
        format_output: str = "geojson",
        bring_out_idf: bool = True,
        quiet: bool = MAPSHAPER_QUIET,
    ):
        """
        Bring ultramarine territories closer to France. This method is executed
        **IN PLACE** and the attribute self.main_file will reference the new
        geodataset.

        Parameters
        ----------
        level_agreg : str, optional
            The desired agregation. The default is "DEPARTEMENT".
            Should be among ['AIRE_ATTRACTION_VILLES', 'BASSIN_VIE',
             'DEPARTEMENT', 'EMPRISES', 'REGION', 'UNITE_URBAINE',
             'ZONE_EMPLOI']
        format_output : str, optional
            The desired output format (which will also be used for intermediate
            files creation). The default is "geojson".
        bring_out_idf : bool, optional
            If True, will extract IdF and zoom on it. The default is True.
        quiet : bool, optional
            If True, inhibits console messages. The default is MAPSHAPER_QUIET.

        Returns
        -------
        None.

        """

        out = mapshaper_bring_closer(
            input_file=f"{self.local_dir}/{self.main_filename}",
            bring_out_idf=bring_out_idf,
            output_dir=self.local_dir,
            output_name="idf_combined",
            output_format=format_output,
            level_agreg=level_agreg,
            quiet=quiet,
        )
        self._substitute_main_file(out)

    def split_file(
        self,
        split_variable: str,
        crs: int = 4326,
        format_output: str = "geojson",
        simplification: int = 0,
        quiet: bool = MAPSHAPER_QUIET,
        **kwargs,
    ) -> list[Self]:
        """
        Split a file into singletons, based on one field (including
        reprojection, simplification and format conversion if need be)

        Parameters
        ----------
        split_variable : str
            Variable to split files onto
        crs : int, optional
            EPSG to project the splitted file onto. The default is 4326.
        format_output : str, optional
            Choosen format to write the output on. The default is "geojson".
        simplification : int, optional
            Degree of simplification. The default is 0.
        quiet : bool, optional
            If True, inhibits console messages. The default is MAPSHAPER_QUIET.
        kwargs :
            Optional values for ConfigDict to ensure the correct generation of
            the afferant geodatasets. For instance, `borders='DEPARTEMENT`

        Returns
        -------
        list[S3GeoDataset]
            return a list of S3GeoDataset objects

        """

        if simplification != 0:
            option_simplify = (
                f"-simplify {simplification}% interval=.5 -clean "
            )
        else:
            option_simplify = ""

        files = mapshaper_split(
            input_file=f"{self.local_dir}/{self.main_filename}",
            layer_name="",
            split_variable=split_variable,
            output_dir=f"{self.local_dir}/splitted",
            output_format=format_output,
            crs=crs,
            option_simplify=option_simplify,
            quiet=quiet,
        )

        geodatasets = []

        for file in files:
            new_config = deepcopy(self.config)
            new_config.update(kwargs)
            new_config.update(
                {
                    "crs": crs,
                    "value": os.path.basename(file).replace(
                        f".{format_output}", ""
                    ),
                    "vectorfile_format": format_output,
                    "simplification": simplification,
                }
            )

            geodatasets.append(
                from_file(
                    file_path=file,
                    fs=self.fs,
                    **new_config,
                ).copy()
            )

        return geodatasets

    def create_downstream_geodatasets(
        self,
        metadata: S3Dataset,
        init_geometry_level="IRIS",
        dissolve_by="COMMUNE",
        niveau_agreg="DEPARTEMENT",
        simplification=0,
    ) -> List[Self]:
        """
        TODO : update docstring (arguments also)
        Create "children" geodatasets based on arguments and send them to S3.

        Do the following processes:
            - join the current geodataset with the metadata to enrich it;
            - dissolve geometries if init_geometry_level != dissolve_by
            - bring ultramarine territories closer
              if niveau_agreg == "FRANCE_ENTIERE_DROM_RAPPROCHES"
            - extract IDF if niveau_agreg=="FRANCE_ENTIERE_IDF_DROM_RAPPROCHES"
            - split the geodataset based on niveau_agreg
            - project the geodataset into the given CRS
            - convert the file into the chosen output
            - upload those datasets to S3 storage system

        The "children" may result to a single file depending of niveau_agreg.

        Note that some of those steps are done **IN PLACE** on the parent
        geodataset (enrichment, dissolution, agregation). Therefore, the
        geodataset should not be re-used after a call to this method.

        Parameters
        ----------
        metadata : S3Dataset
            The metadata file to use to enrich the geodataset
        format_output : str, optional
            The output format, by default "geojson".
        init_geometry_level : str, optional
            The level of basic mesh for the geometries. The default is IRIS.
            Should be among ['IRIS', 'CANTON', 'ARRONDISSEMENT_MUNICIPAL']
        dissolve_by : str, optional
            The level of basic mesh for the geometries. The default is COMMUNE.
            Should be among [
                'REGION', 'DEPARTEMENT', 'BASSIN_VIE',
                'AIRE_ATTRACTION_VILLES', 'UNITE_URBAINE', 'ZONE_EMPLOI',
                'TERRITOIRE', 'ARRONDISSEMENT_MUNICIPAL', 'EPCI', 'EPT',
                ]
        niveau_agreg : str, optional
            The level of aggregation for splitting the dataset into singletons,
            by default "DEPARTEMENT".
            Should be among ['REGION', 'DEPARTEMENT', 'FRANCE_ENTIERE',
             'FRANCE_ENTIERE_DROM_RAPPROCHES', 'LIBELLE_REGION',
             'LIBELLE_DEPARTEMENT', 'BASSIN_VIE', 'AIRE_ATTRACTION_VILLES',
             'UNITE_URBAINE', 'ZONE_EMPLOI', 'TERRITOIRE']
        crs : int, optional
            The coordinate reference system (CRS) code to project the children
            datasets into. By default 4326.
        simplification : int, optional
            The degree of wanted simplification, by default 0.

        Returns
        -------
        List[S3GeoDataset]
            The output path of the processed and split shapefiles.

        """

        output_crs_conf = [
            {"epsg": x[0], "format_output": x[1]}
            for x in product(PIPELINE_CRS, PIPELINE_FORMATS)
        ]

        niveau_agreg = niveau_agreg.upper()
        init_geometry_level = init_geometry_level.upper()

        simplification = simplification if simplification else 0

        # Enrich files with metadata (COG, etc.)

        available_columns = set(self._get_columns()) | set(
            metadata._get_columns()
        )

        if init_geometry_level == "IRIS":
            keys = ["CODE_IRIS", "CODE_IRIS"]
            drop = ["ID", "NOM_COM"]
        elif init_geometry_level == "ARRONDISSEMENT_MUNICIPAL":
            keys = ["INSEE_ARM", "INSEE_ARM"]
            drop = [
                "POPULATION",
                "ID",
                "NOM_M",
            ]
        elif init_geometry_level == "COMMUNE":
            keys = ["INSEE_COM", "INSEE_COM"]
            drop = [
                "POPULATION",
                "ID",
                "NOM_M",
            ]
        elif init_geometry_level == "CANTON":
            keys = ["CAN", "CAN"]
            drop = ["ID"]
            self.add_field("CAN", "INSEE_DEP+INSEE_CAN")
        else:
            # TODO if new base mesh
            pass

        if len(set(keys) & available_columns) < len(set(keys)):
            raise ValueError(
                f"keys must be among {available_columns}, "
                f"found {set(keys)} instead"
            )

        if len(set(drop) & available_columns) < len(drop):
            missing = set(drop) - available_columns
            raise ValueError(
                f"drop must be among {available_columns}, following columns "
                f"are missing : {missing}"
            )

        dtype = set(keys) | {
            "SIREN_EPCI",
            "SIREN_COMMUNE",
            "INSEE_DEP",
            "INSEE_REG",
            "CAN",
            "BURCENTRAL",
            "REG",
            "ZE[0-9]{4}",
            "TUU[0-9]{4}",
            "TDUU[0-9]{4}",
            "TAAV[0-9]{4}",
            "TDAAV[0-9]{4}",
            "CATEAAV[0-9]{4}",
        }
        dtype = {
            col: "str"
            for x in dtype
            for col in available_columns
            if re.match(x, col)
        }

        self.enrich(
            metadata_file=metadata,
            keys=keys,
            dtype=dtype,
            drop=drop,
            rename={},
            format_output=INTERMEDIATE_FORMAT,
        )

        logger.info("new columns are %s", self._get_columns())

        if init_geometry_level != dissolve_by:
            # Dissolve geometries if desired (will replace the local file
            # geodata file based on a communal mesh with  one using the desired
            # mesh

            # Dissolve by both dissolve_by AND niveau_agreg to ensure both
            # dissolution and splitability
            gdf = self.to_frame()
            available_columns = gdf.columns.tolist()
            by = self.find_column_name(dissolve_by, available_columns)
            if niveau_agreg not in (
                "FRANCE_ENTIERE_DROM_RAPPROCHES",
                "FRANCE_ENTIERE_IDF_DROM_RAPPROCHES",
            ):
                aggreg_col = self.find_column_name(
                    niveau_agreg, available_columns
                )
            else:
                aggreg_col = "AREA"
            keys = [by, aggreg_col]

            # And keep all columns which are identical in each subgroup after
            # dissolution + summable columns
            keep = (
                gdf.drop("geometry", axis=1)
                .groupby(keys, dropna=False)
                .nunique()
                == 1
            ).all()
            keep = keep[keep].index.tolist()

            calc = []
            pops = [
                x for x in available_columns if re.match("POPULATION.*", x)
            ]
            if pops:
                calc += [f"{x}=sum({x})" for x in pops]
            if "IDF" in available_columns:
                calc += ["IDF=max(IDF)"]

            by_keys = [by, aggreg_col]

            self.dissolve(
                by=by_keys,
                copy_fields=keep,
                calc=calc,
                format_output=INTERMEDIATE_FORMAT,
            )

        # Bring ultramarine territories closer to France if needed
        if niveau_agreg in (
            "FRANCE_ENTIERE_DROM_RAPPROCHES",
            "FRANCE_ENTIERE_IDF_DROM_RAPPROCHES",
        ):
            self.bring_drom_closer(
                level_agreg=dissolve_by,
                format_output=INTERMEDIATE_FORMAT,
                bring_out_idf=(
                    niveau_agreg == "FRANCE_ENTIERE_IDF_DROM_RAPPROCHES"
                ),
            )

        # Split datasets, based on the desired "niveau_agreg" and proceed to
        # desired level of simplification
        columns = self._get_columns()
        split_by = self.find_column_name(niveau_agreg, columns)

        new_datasets = self.split_file(
            crs=4326,
            format_output=INTERMEDIATE_FORMAT,
            simplification=simplification,
            split_variable=split_by,
            filter_by=niveau_agreg,
            borders=dissolve_by,
        )

        # fix config for storage on S3
        dataset_family = {"dataset_family": "production"}
        [dset.config.update(dataset_family) for dset in new_datasets]

        new_datasets = [
            dset.copy().to_format(**config)
            for dset in new_datasets
            for config in output_crs_conf
        ]
        [dset.update_s3_path_evaluation() for dset in new_datasets]

        # Upload new datasets to S3
        with ExitStack() as stack:
            # enter context for each new dataset instead of looping to allow
            # for multithreading (cleaned locally at exitstack anyway)
            [stack.enter_context(dset) for dset in new_datasets]

            if THREADS_DOWNLOAD > 1:
                threads = min(THREADS_DOWNLOAD, len(new_datasets))
                with ThreadPool(threads) as pool:

                    def upload(dset):
                        return dset.to_s3()

                    list(pool.map(upload, new_datasets).result())
            else:
                [dset.to_s3() for dset in new_datasets]

        return new_datasets

    def only_ultramarine_territories(
        self, quiet: bool = MAPSHAPER_QUIET
    ) -> Self:
        """
        Extracts only ultramarine territories from the given IRIS file and
        dissolve it to cities.

        Parameters
        ----------
        quiet : bool, optional
            If True, inhibits console messages. The default is MAPSHAPER_QUIET.

        Returns
        -------
        S3GeoDataset : new object with only the subset for COM

        """
        iris_file = f"{self.local_dir}/{self.main_filename}"
        tom = mapshaper_capture_cities_from_ultramarine_territories(
            input_city_file=iris_file,
            output_dir=f"{self.local_dir}/tom",
            output_name="TOM",
            output_format=INTERMEDIATE_FORMAT,
            quiet=quiet,
        )
        new_config = deepcopy(self.config)
        new_config.update(
            {"filter_by": "COLLECTIVITE_OUTRE_MER", "value": "France"}
        )
        tom = from_file(file_path=tom, **new_config)

        gdf = tom.to_frame()
        available_columns = gdf.columns.tolist()
        by = self.find_column_name("COMMUNE", available_columns)
        # keep all columns which are identical in each subgroup after
        # dissolution + summable columns (like pop)
        keep = (
            gdf.drop("geometry", axis=1).groupby([by]).nunique() == 1
        ).all()
        keep = keep[keep].index.tolist()

        calc = []
        pops = [x for x in available_columns if re.match("POPULATION.*", x)]
        if pops:
            calc += [f"{x}=sum({x})" for x in pops]
        if "IDF" in available_columns:
            calc += ["IDF=max(IDF)"]

        tom.dissolve(
            by=[by],
            copy_fields=keep,
            calc=calc,
            format_output=INTERMEDIATE_FORMAT,
        )
        return tom

    def substitute_municipal_districts(
        self,
        communal_districts: Self,
        format_output: str = "geojson",
        quiet: bool = MAPSHAPER_QUIET,
    ) -> Self:
        """
        Create a new composite S3GeoDataset from communal districts (Paris,
        Lyon and Marseille) and other "classical" cities (having no communal
        districts)

        Parameters
        ----------
        communal_districts : S3GeoDataset
            S3GeoDataset representing the communal districts (should be
            already downloaded, so this should be generated through a with
            statement).
        format_output : str, optional
            Desired output format. The default is "geojson".
        quiet : bool, optional
            If True, inhibits console messages. The default is MAPSHAPER_QUIET.

        Returns
        -------
        S3GeoDataset
            New S3GeoDataset object reprensenting the dataset. This dataset is
            **NOT** sent to S3.

        """

        # preprocess cities : remove cities having communal districts
        city_file = f"{self.local_dir}/{self.main_filename}"
        city_file = mapshaper_remove_cities_with_districts(
            input_city_file=city_file,
            output_dir=f"{self.local_dir}/singles",
            output_name="COMMUNE",
            output_format=INTERMEDIATE_FORMAT,
            quiet=quiet,
        )

        # note : communal_districts has it's self local_dir which should be
        # in f"{self.local_dir}/{communal_districts.config['territory']}" !
        communal_districts.reproject(format_output=format_output, epsg=4326)
        communal_districts_file = (
            f"{communal_districts.local_dir}/"
            f"{communal_districts.main_filename}"
        )

        communal_districts_file = mapshaper_process_communal_districts(
            input_communal_districts_file=communal_districts_file,
            output_dir=f"{self.local_dir}/districts",
            output_name="ARRONDISSEMENT_MUNICIPAL",
            output_format=INTERMEDIATE_FORMAT,
            quiet=quiet,
        )

        # MERGE CITIES AND ARRONDISSEMENT
        composite = mapshaper_combine_districts_and_cities(
            input_city_file=city_file,
            input_communal_districts_file=communal_districts_file,
            output_dir=self.local_dir,
            output_name="ARRONDISSEMENT_MUNICIPAL",
            output_format=format_output,
            quiet=quiet,
        )

        # move file to new tempdir to isolate this file for new S3GeoDataset
        new_tempdir = tempfile.mkdtemp()
        shutil.move(composite, composite.replace(self.local_dir, new_tempdir))
        composite = composite.replace(self.local_dir, new_tempdir)

        os.unlink(city_file)
        os.unlink(os.path.join(self.local_dir, self.main_filename))

        new_config = deepcopy(self.config)
        new_config.update({"borders": "ARRONDISSEMENT_MUNICIPAL"})
        new_dataset = from_file(file_path=composite, **new_config)

        return new_dataset


def from_frame(
    gdf: gpd.GeoDataFrame,
    fs: S3FileSystem = FS,
    **config: ConfigDict,
) -> S3GeoDataset:
    """
    Create a new S3GeoDataset from a GeoDataFrame, config and fs.

    The new object will write the the geodataframe into a new tempdir; this
    tempdir will be cleaned at __exit__ method's execution. Therefore, the new
    object should be created with a with statement, for instance:
    >>> new_dset = geodataset.from_frame(gdf, fs, **config) as new_file:
    >>> with new_dset:
    >>>     print(new_file)

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to construct the S3GeoDataset from.
    fs : S3FileSystem, optional
        The S3FileSytem to use for storage. The default is FS.
    **config : ConfigDict
        Other arguments to define the path on the S3 to the dataset.

    Returns
    -------
    dset : S3GeoDataset
        New S3GeoDataset object.

    """

    extension = config.get("vectorfile_format", INTERMEDIATE_FORMAT)
    filename = config.get("filename", None)
    if not filename:
        filename = config.get("borders", "file")
    if "." not in filename:
        filename = f"{filename}.{extension}"
    with tempfile.TemporaryDirectory() as tempdir:
        gdf.to_file(f"{tempdir}/{filename}")
        dset = from_file(f"{tempdir}/{filename}", fs, **config)

    return dset


def from_file(
    file_path: str,
    fs: S3FileSystem = FS,
    **config: ConfigDict,
) -> S3GeoDataset:
    """
    Create a new S3GeoDataset from a local file, config and fs.

    The new object will copy the file(s) into a new tempdir; this tempdir will
    be cleaned at __exit__ method's execution. Therefore, the new object should
    be created with a with statement, for instance:
    >>> new_dset = geodataset.from_file("blah.txt", fs, **config) as new_file:
    >>> with new_dset:
    >>>     print(new_file)

    Parameters
    ----------
    file_path : str
        Path to the geodataset file to instantiate the new S3GeoDataset.
    fs : S3FileSystem, optional
        The S3FileSytem to use for storage. The default is FS.
    **config : ConfigDict
        Other arguments to define the path on the S3 to the dataset.

    Returns
    -------
    dset : S3GeoDataset
        New S3GeoDataset object.

    """
    if not os.path.exists(file_path):
        raise ValueError("file not found from local path")

    local_dir = os.path.dirname(file_path)
    filename = os.path.basename(file_path)
    vectorfile_format = filename.rsplit(".", maxsplit=1)[1]

    for key in "filename", "vectorfile_format":
        try:
            del config[key]
        except KeyError:
            pass

    # Create a new S3GeoDataset
    dset = S3GeoDataset(
        fs=fs,
        filename=filename,
        vectorfile_format=vectorfile_format,
        build_from_local=file_path,
        **config,
    )
    dset.local_dir = local_dir
    dset.main_filename = filename

    # Then create a copy to ensure the creation of a new tempdir
    dset = dset.copy()

    return dset


def concat_s3geodataset(
    datasets: List[S3GeoDataset],
    output_name: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    output_dir: str = "temp",
    fs: S3FileSystem = FS,
    quiet: bool = MAPSHAPER_QUIET,
    **config_new_dset: ConfigDict,
) -> S3GeoDataset:
    """
    Concatenate S3GeoDataset in the manner of a geopandas.concat using
    mapshaper. The result is a new S3GeoDataset which will **NOT** be uploaded
    on S3.

    Parameters
    ----------
    datasets : List[S3GeoDataset]
        The list of S3GeoDataset instances to concatenate.
    output_name: str, optional
        The name of the output layer. The default is 'COMMUNE'.
    vectorfile_format : str, optional
        The file format to use for creating the new S3GeoDataset. The default
        is "geojson".
    output_dir : str, optional
        The temporary file used for processing the concatenation. The default
        is "temp".
    fs : S3FileSystem, optional
        The S3FileSystem used ultimately to upload the new S3GeoDataset. The
        default is FS.
    quiet : bool, optional
        If True, inhibits console messages. The default is MAPSHAPER_QUIET.
    **config_new_dset : ConfigDict
        Configuration reprensenting the new S3GeoDataset (used for initiation).
        This will determine the path on the S3FileSystem during storage.

    Returns
    -------
    S3GeoDataset
        New concatenated S3GeoDataset

    """

    for k, dset in enumerate(datasets):
        destination = os.path.join(output_dir, f"{k}.{vectorfile_format}")

        if os.path.exists(os.path.join(dset.local_dir, dset.main_filename)):
            # already downloaded, but not sure of the current projection
            dset.reproject(format_output=vectorfile_format, epsg=4326)

            shutil.copy(
                os.path.join(dset.local_dir, dset.main_filename), destination
            )
        else:
            with dset:
                dset.reproject(format_output=vectorfile_format, epsg=4326)
                shutil.copy(
                    os.path.join(dset.local_dir, dset.main_filename),
                    destination,
                )

    old_files = glob(f"{output_dir}/*.{vectorfile_format}")

    output_path = mapshaper_concat(
        input_dir=output_dir,
        input_format=vectorfile_format,
        output_dir=f"{output_dir}/preprocessed_combined",
        output_name=output_name,
        output_format=vectorfile_format,
        quiet=quiet,
    )

    logger.info("new S3GeoDataset created at %s", output_path)

    for file in old_files:
        os.unlink(file)

    file = glob(f"{output_dir}/preprocessed_combined/*.{vectorfile_format}")[0]
    new_dset = from_file(file_path=file, fs=fs, **config_new_dset)

    return new_dset
