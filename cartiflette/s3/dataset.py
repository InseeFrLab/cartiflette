#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classe générique pour travailler autour d'un dataset présent sur le S3
"""

from copy import deepcopy
from glob import glob
import logging
import os
import shutil
import subprocess
from tempfile import TemporaryDirectory
from typing import List

try:
    from typing import Self
except ImportError:
    # python < 3.11
    Self = "S3GeoDataset"
import warnings

from s3fs import S3FileSystem


from cartiflette.config import FS
from cartiflette.utils import (
    create_path_bucket,
    ConfigDict,
    DICT_CORRESP_ADMINEXPRESS,
)
from cartiflette.mapshaper import (
    mapshaper_convert_mercator,
    mapshaper_enrich,
    mapshaper_bring_closer,
    mapshaper_split,
)


logger = logging.getLogger(__name__)


class S3Dataset:
    """
    Base class representing a dataset stored on the S3

    This class is used on it's own only for tabular datasets (to be joined to
    S3GeoDataset for enrichment)
    """

    files = None
    main_filename = None
    s3_dirpath = None

    def __init__(
        self,
        fs: S3FileSystem = FS,
        local_dir: str = "temp",
        filename: str = "*",
        **config: ConfigDict,
    ):
        """
        Create a S3Dataset.

        Parameters
        ----------
        fs : S3FileSystem, optional
            S3FileSystem used for storage. The default is FS.
        local_dir : str, optional
            Local directory used for transformations using mapshaper. The
            default is "temp".
        filename : str, optional
            In case there are multiple files into the same folder define it
            to avoid catching the wrong file from S3FileSystem
            (this should only occur with the download of raw datasets with
             COMMUNE.shp and ARRONDISSEMENT_MUNICIPAL.shp being stored in the
             same directory).
            The default is "*".
            For instance, "COMMUNE.shp"
        **config : ConfigDict
            Other arguments to define the path on the S3 to the dataset.
        """
        self.fs = fs
        self.config = config
        self.local_dir = local_dir
        self.local_files = []

        self.filename = filename.rsplit(".", maxsplit=1)[0]

        self.get_path_of_dataset()

        self.source = (
            f"{config.get('provider', '')}:{config.get('source', '')}"
        )

    def __str__(self):
        return f"<cartiflette.s3.dataset.S3Dataset({self.config})>"

    def __repr__(self):
        return self.__str__()

    def get_path_of_dataset(self):
        "retrieve dataset's full paths on S3"
        path = os.path.dirname(create_path_bucket(self.config))
        search = f"{path}/**/{self.filename}"
        if self.filename != "*":
            search += ".*"

        self.s3_files = self.fs.glob(search)
        if not self.s3_files:
            warnings.warn(f"this dataset is not available on S3 on {search}")

            return path

        if len(self.s3_files) > 1:
            main_filename = (
                self.s3_files[0].rsplit(".", maxsplit=1)[0] + ".shp"
            )
        else:
            main_filename = self.s3_files[0]

        self.main_filename = os.path.basename(main_filename)
        self.s3_dirpath = os.path.dirname(main_filename)

    def to_s3(self):
        "upload file to S3"
        target = self.s3_dirpath
        if not target.endswith("/"):
            target += "/"
        logger.info("sending %s -> %s", self.local_dir, target)
        self.fs.put(self.local_dir + "/*", target, recursive=True)

    def to_local_folder_for_mapshaper(self):
        "download to local dir and prepare for use with mapshaper"

        if not self.s3_files:
            raise ValueError(
                f"this dataset is not available on S3 : {self.s3_dirpath}"
            )

        self.local_dir = f"{self.local_dir}/{self.config['territory']}"
        os.makedirs(self.local_dir, exist_ok=True)

        files = []

        # Get all files (plural in case of shapefile) from Minio
        logger.info("downloading %s to %s", self.s3_files, self.local_dir)
        for file in self.s3_files:
            path = f"{self.local_dir}/{file.rsplit('/', maxsplit=1)[-1]}"
            self.fs.download(file, path)
            logger.info("file written to %s", path)
            files.append(path)

        self.local_files = files

    def __enter__(self):
        "download file into local folder at enter"
        self.to_local_folder_for_mapshaper()
        return self

    def __exit__(self, *args, **kwargs):
        "remove tempfiles as exit"
        try:
            try:
                shutil.rmtree(os.path.join(self.local_dir))
            except FileNotFoundError:
                pass
        except Exception as exc:
            warnings.warn(exc)


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

    def to_mercator(self, format_intermediate: str = "geojson"):
        "project to mercator using mapshaper"
        mapshaper_convert_mercator(
            filename_initial=self.main_filename,
            local_dir=self.local_dir,
            territory=self.config["territory"],
            identifier=self.config["territory"],
            format_intermediate=format_intermediate,
        )

    def enrich(self, metadata_file: str, dict_corresp: dict):
        "enrich with metadata using mapshaper"
        mapshaper_enrich(
            local_dir=self.local_dir,
            filename_initial=os.path.basename(self.main_filename),
            metadata_file=metadata_file,
            dict_corresp=dict_corresp,
        )

    def dissolve(
        self,
        by: str,
        copy_fields: List[str] = None,
        calc: List[str] = None,
        format_output: str = "geojson",
    ):
        """
        Dissolve geometries and rename local file using mapshaper.

        Dissolve geometries on field `bv`, keeping fields `copy_fields`. Other
        fields should be computaded using javascript functions with `calc`
        argument. The original file will be overwritten, then renamed to
        {by}.{formate_intermediate}. self.main_filename will be updated.


        Parameters
        ----------
        by : str
            Field used to dissolve
        calc : Listr[str], optional
            Fields on which computed should be operated, describing valid js
            functions. For instance ["POPULATION=sum(POPULATION)"]. The default
            is None.
        copy_fields : List[str], optional
            Copies values from the first feature in each group of dissolved
            features. The default is None.
        format_output : str, optional
            Output format. The default is geojson

        Returns
        -------
        None.

        """
        init = f"{self.local_dir}/{self.main_filename}"
        out = f"{self.local_dir}/{by}.{format_output}"

        cmd = (
            f"mapshaper {init} "
            f"name='by' "
            "-proj EPSG:4326 "
            f"-dissolve {by} "
        )
        if calc:
            calc = ",".join(calc)
            cmd += f"calc='{calc}' "
        if copy_fields:
            cmd += "copy-fields=" + ",".join(copy_fields)

        cmd += f" -o {init} force"

        subprocess.run(
            cmd,
            shell=True,
            check=True,
            text=True,
        )
        os.rename(init, out)
        self.main_filename = os.path.basename(out)

    def bring_drom_closer(
        self,
        level_agreg: str = "DEPARTEMENT",
        format_output: str = "geojson",
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

        Returns
        -------
        None.

        """

        init = f"{self.local_dir}/{self.main_filename}"
        filename_output = "idf_combined"
        out = f"{self.local_dir}/{filename_output}.{format_output}"

        mapshaper_bring_closer(
            filename_initial=self.main_filename,
            local_dir=self.local_dir,
            format_intermediate=format_output,
            level_agreg=level_agreg,
            filename_output=f"idf_combined.{format_output}",
        )
        os.unlink(init)
        self.main_filename = os.path.basename(out)

    def split_file(
        self,
        split_variable: str,
        crs: int = 4326,
        format_output: str = "geojson",
        simplification: int = 0,
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
        kwargs :
            Optional values for ConfigDict to ensure the correct generation of
            the afferant geodatasets. For instance, `borders='DEPARTEMENT`

        Returns
        -------
        list[S3GeoDataset]
            return a list of S3GeoDataset objects

        """

        if simplification != 0:
            option_simplify = f"-simplify {simplification}% "
        else:
            option_simplify = ""

        mapshaper_split(
            input_file=f"{self.local_dir}/{self.main_filename}",
            format_output=format_output,
            output_dir=f"{self.local_dir}/splitted",
            crs=crs,
            option_simplify=option_simplify,
            source_identifier=self.source,
            split_variable=split_variable,
        )
        files = glob(f"{self.local_dir}/splitted/*.{format_output}")

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
            # place file into a unique folder
            new_dir = f"{self.local_dir}/splitted/{new_config['value']}"
            os.makedirs(new_dir)
            shutil.move(file, new_dir)

            geodatasets.append(
                S3GeoDataset(
                    fs=self.fs,
                    local_dir=new_dir,
                    **new_config,
                )
            )

        return geodatasets

    def mapshaperize_split(
        self,
        metadata: S3Dataset,
        format_output="geojson",
        niveau_polygons="COMMUNE",
        niveau_agreg="DEPARTEMENT",
        crs=4326,
        simplification=0,
        dict_corresp=None,
    ) -> List[Self]:
        """
        Create "children" geodatasets based on arguments and send them to S3.

        Do the following processes:
            - join the current geodataset with the metadata to enrich it;
            - dissolve geometries if niveau_polygons != "COMMUNE"
            - bring ultramarine territories closer
              if niveau_agreg == "FRANCE_ENTIERE_DROM_RAPPROCHES"
            - split the geodataset based on niveau_agreg
            - project the geodataset into the given CRS
            - convert the file into the chosen output
            - upload those datasets to S3 storage system

        The "children" may result to a single file depending of niveau_agreg.

        Note that some of those steps are done **IN PLACE** on the parent
        geodataset (enrichment, dissolution, agregation). Therefore, the
        geodataset should not be used after a call to this method.

        Parameters
        ----------
        metadata : S3Dataset
            The metadata file to use to enrich the geodataset
        format_output : str, optional
            The output format, by default "geojson".
        niveau_polygons : str, optional
            The level of basic mesh for the geometries. The default is COMMUNE.
            Should be among ['REGION', 'DEPARTEMENT', 'FRANCE_ENTIERE',
             'FRANCE_ENTIERE_DROM_RAPPROCHES', 'LIBELLE_REGION',
             'LIBELLE_DEPARTEMENT', 'BASSIN_VIE', 'AIRE_ATTRACTION_VILLES',
             'UNITE_URBAINE', 'ZONE_EMPLOI', 'TERRITOIRE']
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
        dict_corresp: dict
            A dictionary giving correspondance between niveau_agreg argument
            and variable names. The default is None, which will result to
            DICT_CORRESP_ADMINEXPRESS.

        Returns
        -------
        List[S3GeoDataset]
            The output path of the processed and split shapefiles.

        """

        if not dict_corresp:
            dict_corresp = DICT_CORRESP_ADMINEXPRESS

        niveau_agreg = niveau_agreg.upper()
        niveau_polygons = niveau_polygons.upper()

        simplification = simplification if simplification else 0

        # Enrich files with metadata (COG, etc.)
        self.enrich(
            metadata_file=metadata.local_files[0],
            dict_corresp=dict_corresp,
        )

        if niveau_polygons != "COMMUNE":
            # Dissolve geometries if desired (will replace the local file
            # geodata file based on a communal mesh with  one using the desired
            # mesh

            # Identify which fields should be copied from the first feature in
            # each group of dissolved features:
            copy_fields = [
                dict_corresp[niveau_polygons],
                dict_corresp[niveau_agreg],
                dict_corresp.get(f"LIBELLE_{niveau_polygons}"),
                dict_corresp.get(f"LIBELLE_{niveau_agreg}"),
            ]
            copy_fields = [x for x in copy_fields if x]

            self.dissolve(
                by=dict_corresp[niveau_polygons],
                copy_fields=copy_fields,
                calc=["POPULATION=sum(POPULATION)"],
                format_output=format_output,
            )

        # Bring ultramarine territories closer to France if needed
        if niveau_agreg == "FRANCE_ENTIERE_DROM_RAPPROCHES":
            niveau_filter_drom = "DEPARTEMENT"
            if niveau_polygons != "COMMUNE":
                niveau_filter_drom = niveau_polygons

            self.bring_drom_closer(
                level_agreg=niveau_filter_drom,
                format_output=format_output,
            )

        # Split datasets, based on the desired "niveau_agreg" and proceed to
        # desired level of simplification
        new_datasets = self.split_file(
            crs=crs,
            format_output=format_output,
            simplification=simplification,
            split_variable=dict_corresp[niveau_agreg],
            filter_by=niveau_agreg,
            borders=niveau_polygons,
        )

        # Upload new datasets to S3
        for dataset in new_datasets:
            dataset.to_s3()

        return new_datasets

    def mapshaperize_split_merge(
        self,
        format_output="topojson",
        niveau_agreg="DEPARTEMENT",
        provider="IGN",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        territory="metropole",
        config_file_city={},
        config_file_arrondissement={},
        local_dir="temp",
        crs=4326,
        simplification=0,
        dict_corresp=None,
    ):
        # TODO

        if not dict_corresp:
            dict_corresp = DICT_CORRESP_ADMINEXPRESS

        niveau_agreg = niveau_agreg.upper()

        simplification = simplification if simplification else 0

        # City level borders, file location
        directory_city = config_file_city.get("location", local_dir)
        initial_filename_city = config_file_city.get("filename", "COMMUNE")
        extension_initial_city = config_file_city.get("extension", "shp")

        # Arrondissement level borders, file location
        directory_arrondissement = config_file_arrondissement.get(
            "location", local_dir
        )
        initial_filename_arrondissement = config_file_arrondissement.get(
            "filename", "ARRONDISSEMENT_MUNICIPAL"
        )
        extension_initial_arrondissement = config_file_arrondissement.get(
            "extension", "shp"
        )

        # Intermediate output location
        output_path = f"{local_dir}/{territory}/{niveau_agreg}/{format_output}/{simplification=}"

        if simplification != 0:
            option_simplify = f"-simplify {simplification}% "
        else:
            option_simplify = ""

        format_intermediate = "geojson"

        # PREPROCESS CITIES
        file_city = f"{directory_city}/{initial_filename_city}.{extension_initial_city}"
        subprocess.run(
            (
                f"mapshaper {file_city} name='COMMUNE' "
                f"-proj EPSG:4326 "
                f"-filter '\"69123,13055,75056\".indexOf(INSEE_COM) > -1' invert "
                f'-each "INSEE_COG=INSEE_COM" '
                f"-o {output_path}/communes_simples.{format_intermediate} "
                f'format={format_intermediate} extension=".{format_intermediate}" singles'
            ),
            shell=True,
            check=True,
            text=True,
        )

        # PREPROCESS ARRONDISSEMENT
        file_arrondissement = (
            f"{directory_arrondissement}/"
            f"{initial_filename_arrondissement}.{extension_initial_arrondissement}"
        )
        subprocess.run(
            (
                f"mapshaper {file_arrondissement} "
                f"name='ARRONDISSEMENT_MUNICIPAL' "
                f"-proj EPSG:4326 "
                f"-rename-fields INSEE_COG=INSEE_ARM "
                f"-each 'STATUT=\"Arrondissement municipal\" ' "
                f"-o {output_path}/arrondissements.{format_intermediate} "
                f'format={format_intermediate} extension=".{format_intermediate}"'
            ),
            shell=True,
            check=True,
            text=True,
        )

        # MERGE CITIES AND ARRONDISSEMENT
        subprocess.run(
            (
                f"mapshaper "
                f"{output_path}/communes_simples.{format_intermediate} "
                f"{output_path}/arrondissements.{format_intermediate} snap combine-files "
                f"-proj EPSG:4326 "
                f"-rename-layers COMMUNE,ARRONDISSEMENT_MUNICIPAL "
                f"-merge-layers target=COMMUNE,ARRONDISSEMENT_MUNICIPAL force "
                f"-rename-layers COMMUNE_ARRONDISSEMENT "
                f"-o {output_path}/raw.{format_intermediate} "
                f'format={format_intermediate} extension=".{format_intermediate}"'
            ),
            shell=True,
            check=True,
            text=True,
        )

        # STEP 1: ENRICHISSEMENT AVEC COG
        mapshaper_enrich(
            local_dir=output_path,
            filename_initial="raw",
            extension_initial=format_intermediate,
            output_path=f"{output_path}/raw2.{format_intermediate}",
            dict_corresp=DICT_CORRESP_ADMINEXPRESS,
        )

        input_path = f"{output_path}/raw2.{format_intermediate}"

        if niveau_agreg.upper() == "FRANCE_ENTIERE_DROM_RAPPROCHES":
            input_path = mapshaper_bring_closer(input_path)

        # TRANSFORM AS NEEDED
        mapshaper_split(
            input_file=input_path,
            layer_name="",
            split_variable=dict_corresp[niveau_agreg],
            output_path=output_path,
            format_output=format_output,
            crs=crs,
            option_simplify=option_simplify,
            source_identifier=f"{provider}:{source}",
        )

        return output_path


def concat(
    datasets: List[S3GeoDataset],
    vectorfile_format: str = "geojson",
    fs: S3FileSystem = FS,
    **config_new_dset: ConfigDict,
) -> S3GeoDataset:
    """
    Concatenate S3GeoDataset in the manner of a geopandas.concat using
    mapshaper. The result is a new S3GeoDataset which will be uploaded on S3
    at the end.

    Parameters
    ----------
    datasets : List[S3GeoDataset]
        The list of S3GeoDataset instances to concatenate.
    vectorfile_format : str, optional
        The file format to use for creating the new S3GeoDataset. The default
        is "geojson".
    fs : S3FileSystem, optional
        The S3FileSystem used ultimately to upload the new S3GeoDataset. The
        default is FS.
    **config_new_dset : ConfigDict
        Configuration reprensenting the new S3GeoDataset (used for initiation).
        This will determine the path on the S3FileSystem during storage.

    Returns
    -------
    S3GeoDataset
        New S3GeoDataset being the concatenation of .

    """
    with TemporaryDirectory() as tempdir:
        for k, dset in enumerate(datasets):
            with dset:
                dset.to_mercator(format_intermediate="topojson")
                shutil.copytree(
                    dset.local_dir + "/preprocessed", f"{tempdir}/{k}"
                )

        output_path = (
            f"{tempdir}/preprocessed_combined/COMMUNE.{vectorfile_format}"
        )
        subprocess.run(
            (
                f"mapshaper -i {tempdir}/**/"
                f"*.{vectorfile_format}"
                " combine-files name='COMMUNE' "
                f"-proj EPSG:4326 "
                f"-merge-layers "
                f"-o {output_path} "
                f"format={vectorfile_format} "
                f'extension=".{vectorfile_format}" singles'
            ),
            shell=True,
            check=True,
            text=True,
        )

        logger.info("new S3GeoDataset created at %s", output_path)

        new_dset = S3GeoDataset(
            fs,
            local_dir=f"{tempdir}/preprocessed_combined",
            **config_new_dset,
        )
        new_dset.to_s3()

        return new_dset


# if __name__ == "__main__":
#     with S3GeoDataset(
#         bucket=BUCKET,
#         path_within_bucket=PATH_WITHIN_BUCKET,
#         provider="IGN",
#         dataset_family="ADMINEXPRESS",
#         source="EXPRESS-COG-TERRITOIRE",
#         year=2024,
#         borders=None,
#         crs="*",
#         filter_by="origin",
#         value="raw",
#         vectorfile_format="shp",
#         territory="mayotte",
#         simplification=0,
#     ) as dset:
#         dset.to_mercator()
