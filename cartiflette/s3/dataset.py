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
    Self = "BaseGISDataset"
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

        self.source = (
            f"{config.get('provider', '')}:{config.get('source', '')}"
        )

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
            self.main_filename = os.path.basename(
                self.s3_files[0].rsplit(".", maxsplit=1)[0] + ".shp"
            )
        else:
            self.main_filename = os.path.basename(self.s3_files[0])

        # return exact path (without glob expression):
        return os.path.dirname(self.main_filename)

    def to_s3(self):
        "upload file to S3"
        target = self.s3_dirpath
        if not target.endswith("/"):
            target += "/"
        logger.warning(f"{self.local_dir} -> {target}")
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
        format_intermediate: str = "geojson",
    ):
        init = f"{self.local_dir}/{self.main_filename}"
        filename_output = "idf_combined"
        out = f"{self.local_dir}/{filename_output}.{format_intermediate}"

        mapshaper_bring_closer(
            filename_initial=self.main_filename,
            local_dir=self.local_dir,
            format_intermediate=format_intermediate,
            level_agreg=level_agreg,
            filename_output=f"idf_combined.{format_intermediate}",
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
        Split a file into singleton, based on one field (including
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
        list[BaseGISDataset]
            return a list of BaseGISDataset objects

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
                BaseGISDataset(
                    fs=self.fs,
                    intermediate_dir=new_dir,
                    **new_config,
                )
            )

        return geodatasets

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

        # TODO
        niveau_polygons:
            a priori la géométrie souhaitée au final ?

        niveau_agreg : str, optional
            The level of aggregation for the split, by default "DEPARTEMENT".
            A priori le niveau de filtre ???


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

        niveau_agreg = niveau_agreg.upper()
        niveau_polygons = niveau_polygons.upper()

        simplification = simplification if simplification else 0

        # STEP 1: ENRICHISSEMENT AVEC COG
        self.enrich(
            metadata_file=metadata.local_files[0],
            dict_corresp=dict_corresp,
        )

        if niveau_polygons != "COMMUNE":
            # STEP 1B: DISSOLVE GEOMETRIES IF NEEDED (GENERATING GDF WITH
            # NON-CITIES GEOMETRIES)

            # Identify which fields should be copied from the first feature in
            # each group of dissolved features.
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

        # IF WE DESIRE TO BRING "DROM" CLOSER TO FRANCE
        if niveau_agreg == "FRANCE_ENTIERE_DROM_RAPPROCHES":
            niveau_filter_drom = "DEPARTEMENT"
            if niveau_polygons != "COMMUNE":
                niveau_filter_drom = niveau_polygons

            self.bring_drom_closer(
                level_agreg=niveau_filter_drom,
                format_intermediate=format_output,
            )

        # STEP 2: SPLIT ET SIMPLIFIE
        new_datasets = self.split_file(
            crs=crs,
            format_output=format_output,
            simplification=simplification,
            split_variable=dict_corresp[niveau_agreg],
            filter_by=niveau_agreg,
            borders=niveau_polygons,
        )

        for dataset in new_datasets:
            dataset.to_s3()


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
