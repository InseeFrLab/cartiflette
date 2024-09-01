#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classe générique pour travailler autour d'un dataset présent sur le S3
"""

import logging
import os
import shutil
import subprocess
from tempfile import TemporaryDirectory
import warnings

import pandas as pd
from s3fs import S3FileSystem


from cartiflette.config import FS
from cartiflette.utils import (
    create_path_bucket,
    ConfigDict,
    DICT_CORRESP_ADMINEXPRESS,
)
from cartiflette.mapshaper import mapshaper_convert_mercator, mapshaper_enrich


logger = logging.getLogger(__name__)


def concat(
    datasets: list = None,
    format_intermediate: str = "topjson",
    fs: S3FileSystem = FS,
    **config_new_dset: ConfigDict,
):
    with TemporaryDirectory() as tempdir:
        for k, dset in enumerate(datasets):
            with dset:
                dset.to_mercator(format_intermediate="topojson")
                shutil.copytree(
                    dset.local_dir + "/preprocessed", f"{tempdir}/{k}"
                )

        output_path = (
            f"{tempdir}/preprocessed_combined/COMMUNE.{format_intermediate}"
        )
        subprocess.run(
            (
                f"mapshaper -i {tempdir}/**/"
                f"*.{format_intermediate}"
                " combine-files name='COMMUNE' "
                f"-proj EPSG:4326 "
                f"-merge-layers "
                f"-o {output_path} "
                f"format={format_intermediate} "
                f'extension=".{format_intermediate}" singles'
            ),
            shell=True,
            check=True,
            capture_output=True,
            text=True,
        )

        print(output_path)

        new_dset = BaseGISDataset(
            fs,
            intermediate_dir=f"{tempdir}/preprocessed_combined",
            **config_new_dset,
        )
        new_dset.to_s3()

        return new_dset


class Dataset:
    files = None

    def __init__(
        self,
        fs: S3FileSystem = FS,
        intermediate_dir: str = "temp",
        **config: ConfigDict,
    ):
        self.fs = fs
        self.config = config
        self.s3_dirpath = self.get_path_of_dataset()
        self.local_dir = intermediate_dir

    def __str__(self):
        return f"<cartiflette.s3.dataset.Dataset({self.config})>"

    def __repr__(self):
        return self.__str__()

    def get_path_of_dataset(self):
        "retrieve dataset's full paths on S3"
        path = os.path.dirname(create_path_bucket(self.config))
        search = f"{path}/**/*"
        self.s3_files = self.fs.glob(search)
        if not self.s3_files:
            warnings.warn(f"this dataset is not available on S3 on {search}")

            return path

        if len(self.s3_files) > 1:
            self.main_filename = (
                self.s3_files[0].rsplit(".", maxsplit=1)[0] + ".shp"
            )
        else:
            self.main_filename = self.s3_files[0].rsplit(".", maxsplit=1)[0]

        # return exact path (without glob expression):
        return os.path.dirname(self.s3_files[0])

    def to_s3(self):
        "upload file to S3"
        target = self.s3_dirpath
        if not target.endswith("/"):
            target += "/"
        logger.warning(self.s3_dirpath, target)
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
        logger.info(f"downloading {self.s3_files} to {self.local_dir}")
        for file in self.s3_files:
            path = f"{self.local_dir}/{file.rsplit('/', maxsplit=1)[-1]}"
            self.fs.download(file, path)
            logger.warning(f"file written to {path}")
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
        except Exception as e:
            warnings.warn(e)


class BaseGISDataset(Dataset):
    files = None

    def __str__(self):
        return f"<cartiflette.s3.dataset.BaseGISDataset({self.config})>"

    def to_mercator(self, format_intermediate: str = "geojson"):
        "project to mercator using mapshaper"
        mapshaper_convert_mercator(
            local_dir=self.local_dir,
            territory=self.config["territory"],
            identifier=self.config["territory"],
            format_intermediate=format_intermediate,
        )

    def enrich(self, metadata_file: str, dict_corresp: dict):
        "enrich with metadata using mapshaper"
        mapshaper_enrich(
            local_dir=self.local_dir,
            filename_initial=self.main_filename,
            metadata_file=metadata_file,
            dict_corresp=dict_corresp,
        )

    def mapshaperize_split(
        self,
        # local_dir="temp",
        # config_file_city={},
        metadata: Dataset,
        format_output="topojson",
        niveau_polygons="COMMUNE",
        niveau_agreg="DEPARTEMENT",
        provider="IGN",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        territory="metropole",
        crs=4326,
        simplification=0,
        dict_corresp=DICT_CORRESP_ADMINEXPRESS,
    ):
        """

        TODO: docstring not up-to-date

        Processes shapefiles and splits them based on specified parameters using Mapshaper.

        Parameters
        ----------
        local_dir : str, optional
            The local directory for file storage, by default "temp".
        filename_initial : str, optional
            The initial filename, by default "COMMUNE".
        extension_initial : str, optional
            The initial file extension, by default "shp".
        format_output : str, optional
            The output format, by default "topojson".
        niveau_agreg : str, optional
            The level of aggregation for the split, by default "DEPARTEMENT".
        provider : str, optional
            The data provider, by default "IGN".
        source : str, optional
            The data source, by default "EXPRESS-COG-CARTO-TERRITOIRE".
        year : int, optional
            The year of the data, by default 2022.
        dataset_family : str, optional
            The dataset family, by default "ADMINEXPRESS".
        territory : str, optional
            The territory of the data, by default "metropole".
        crs : int, optional
            The coordinate reference system (CRS) code, by default 4326.
        simplification : int, optional
            The degree of simplification, by default 0.
        dict_corresp: dict
            A dictionary giving correspondance between niveau_agreg argument
            and variable names.

        Returns
        -------
        str
            The output path of the processed and split shapefiles.

        """

        simplification_percent = (
            simplification if simplification is not None else 0
        )

        # # City level borders, file location
        # directory_city = config_file_city.get("location", local_dir)
        # initial_filename_city = config_file_city.get("filename", "COMMUNE")
        # extension_initial_city = config_file_city.get("extension", "shp")

        output_path = f"{self.local_dir}/{niveau_agreg}/{format_output}/{simplification=}"
        os.makedirs(output_path, exist_ok=True)

        if simplification_percent != 0:
            option_simplify = f"-simplify {simplification_percent}% "
        else:
            option_simplify = ""

        temp_filename = "temp.geojson"

        # STEP 1: ENRICHISSEMENT AVEC COG
        metadata_path = metadata.local_files[0]
        try:
            self.enrich(
                metadata_file=metadata_path,
                dict_corresp=dict_corresp,
            )
        except Exception:
            raise
        finally:
            os.unlink(metadata_path)

        if niveau_polygons != initial_filename_city:
            csv_list_vars = (
                f"{dict_corresp[niveau_polygons]},"
                f"{dict_corresp[niveau_agreg]}"
            )
            libelle_niveau_polygons = dict_corresp.get(
                "LIBELLE_" + niveau_polygons, ""
            )
            if libelle_niveau_polygons != "":
                libelle_niveau_polygons = f",{libelle_niveau_polygons}"
            libelle_niveau_agreg = dict_corresp.get(
                "LIBELLE_" + niveau_agreg, ""
            )
            if libelle_niveau_polygons != "":
                libelle_niveau_agreg = f",{libelle_niveau_agreg}"
            csv_list_vars = f"{csv_list_vars}{libelle_niveau_polygons}{libelle_niveau_agreg}"

            # STEP 1B: DISSOLVE IF NEEDED
            cmd_dissolve = (
                f"mapshaper {temp_filename} "
                f"name='' -proj EPSG:4326 "
                f"-dissolve {dict_corresp[niveau_polygons]} "
                f"calc='POPULATION=sum(POPULATION)' "
                f"copy-fields={csv_list_vars} "
                "-o temp.geojson force"
            )
            subprocess.run(
                cmd_dissolve,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
            )

        # IF WE DESIRE TO BRING "DROM" CLOSER TO FRANCE
        if niveau_agreg.upper() == "FRANCE_ENTIERE_DROM_RAPPROCHES":
            niveau_filter_drom = "DEPARTEMENT"
            if niveau_polygons != "COMMUNE":
                niveau_filter_drom = niveau_polygons
            input_path = mapshaper_bring_closer(
                temp_filename, level_agreg=niveau_filter_drom
            )
        else:
            input_path = "temp.geojson"

        print(input_path)

        # STEP 2: SPLIT ET SIMPLIFIE
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


# if __name__ == "__main__":
#     with BaseGISDataset(
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
