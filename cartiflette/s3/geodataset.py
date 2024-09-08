# -*- coding: utf-8 -*-

from copy import deepcopy
import logging
import os
import shutil
from tempfile import TemporaryDirectory
from typing import List

try:
    from typing import Self
except ImportError:
    # python < 3.11
    Self = "S3GeoDataset"

from s3fs import S3FileSystem


from .dataset import S3Dataset
from cartiflette.mapshaper import (
    mapshaper_convert_mercator,
    mapshaper_enrich,
    mapshaper_bring_closer,
    mapshaper_split,
    mapshaper_dissolve,
    mapshaper_concat,
    mapshaper_remove_cities_with_districts,
    mapshaper_preprocess_communal_districts,
    mapshaper_combine_districts_and_cities,
)
from cartiflette.utils import (
    ConfigDict,
    DICT_CORRESP_ADMINEXPRESS,
)
from cartiflette.config import FS


class S3GeoDataset(S3Dataset):
    """
    Base class representing a geodataset stored on the S3

    An instance can either be an existing file loaded from the S3 or a new
    geodataset in the process of creation. In that case, a warning will be
    displayed at creation to alert that the file is not present on the S3
    (yet).
    """

    # TODO : function "from_file" qui désactive le warning ?

    def __str__(self):
        return f"<cartiflette.s3.dataset.S3GeoDataset({self.config})>"

    def _substitute_main_file(self, new_file):
        if not os.path.dirname(new_file) == self.local_dir:
            raise ValueError(
                f"cannot substitute main_file with {new_file=} and "
                f"{self.local_dir=} : directories are not identical"
            )

        if os.path.basename(new_file) == self.main_filename:
            return

        os.unlink(f"{self.local_dir}/{self.main_filename}")
        self.main_filename = os.path.basename(new_file)

    def to_mercator(self, format_output: str = "geojson"):
        "project to mercator using mapshaper"
        input_file = f"{self.local_dir}/{self.main_filename}.*"

        new_file = mapshaper_convert_mercator(
            input_file=input_file,
            output_dir=self.local_dir,
            output_name=self.main_filename,
            output_format=format_output,
            filter_by=self.config["territory"],
        )
        self._substitute_main_file(new_file)

    def enrich(
        self, metadata_file: str, dict_corresp: dict, format_output: str
    ):
        "enrich with metadata using mapshaper"
        input_geodata = f"{self.local_dir}/{self.main_filename}"
        output = mapshaper_enrich(
            input_geodata_file=input_geodata,
            input_metadata_file=metadata_file,
            output_dir=self.local_dir,
            output_name=self.main_filename,
            output_format=format_output,
        )
        self._substitute_main_file(output)

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
        out = mapshaper_dissolve(
            input_file=init,
            by=by,
            copy_fields=copy_fields,
            calc=calc,
            output_dir=self.local_dir,
            output_name=by,
            output_format=format_output,
        )
        self._substitute_main_file(out)

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

        out = mapshaper_bring_closer(
            input_file=f"{self.local_dir}/{self.main_filename}",
            output_dir=self.local_dir,
            output_name="idf_combined",
            output_format=format_output,
            level_agreg=level_agreg,
        )
        self._substitute_main_file(out)

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

        files = mapshaper_split(
            input_file=f"{self.local_dir}/{self.main_filename}",
            layer_name="",
            split_variable=split_variable,
            output_dir=f"{self.local_dir}/splitted",
            output_format=format_output,
            crs=crs,
            option_simplify=option_simplify,
            source_identifier=self.source,
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

    def create_downstream_geodatasets(
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
        geodataset should not be re-used after a call to this method.

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
             'UNITE_URBAINE', 'ZONE_EMPLOI', 'TERRITOIRE',
             'ARRONDISSEMENT_MUNICIPAL']
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
            format_output=format_output,
        )

        if niveau_polygons not in {"COMMUNE", "ARRONDISSEMENT_MUNICIPAL"}:
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

    def substitute_muncipal_districts(
        self, format_output: str = "geojson"
    ) -> Self:
        """
        Create a new composite S3GeoDataset from communal districts (Paris,
        Lyon and Marseille) and other "classical" cities (having no communal
        districts)

        Parameters
        ----------
        format_output : str, optional
            Desired output format. The default is "geojson".

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
            output_name="communes_simples",
            output_format=format_output,
        )

        # download and preprocess communal districts (ie. ensure proj to 4326)
        new_config = deepcopy(self.config)
        new_config.update(
            {
                "borders": None,
                "crs": 2154,
                "territory": "metropole",
                "vectorfile_format": "shp",
                "filter_by": "origin",
                "value": "raw",
            }
        )
        communal_districts = S3GeoDataset(
            fs=self.fs,
            local_dir=self.local_dir,
            filename="ARRONDISSEMENT_MUNICIPAL",
            **new_config,
        )
        communal_districts.to_local_folder_for_mapshaper()

        # note : communal_districts has it's self local_dir which should be
        # in f"{self.local_dir}/{communal_districts.config['territory']}" !
        communal_districts.to_mercator(format_output=format_output)
        communal_districts_file = (
            f"{communal_districts.local_dir}/preprocessed/"
            + communal_districts.main_filename
        )

        communal_districts_file = mapshaper_preprocess_communal_districts(
            input_communal_districts_file=communal_districts_file,
            output_dir=f"{self.local_dir}/districts",
            output_name="arrondissements",
            output_format=format_output,
        )

        # MERGE CITIES AND ARRONDISSEMENT
        mapshaper_combine_districts_and_cities(
            input_city_file=city_file,
            input_communal_districts_file=communal_districts_file,
            output_dir=self.local_dir,
            output_name="cities-districts",
            output_format=format_output,
        )

        new_config = deepcopy(self.config)
        new_config.update({"borders": "COMMUNE_ARRONDISSEMENT"})
        new_dataset = S3GeoDataset(
            fs=self.fs, local_dir=self.local_dir, **new_config
        )
        return new_dataset

    def create_downstream_geodatasets_with_districts(
        self,
        metadata: S3Dataset,
        format_output="geojson",
        niveau_agreg="DEPARTEMENT",
        crs=4326,
        simplification=0,
        dict_corresp=None,
    ):
        """
        Create "children" geodatasets based on arguments and send them to S3,
        using a communal districts + cities composite mesh

        Do the following processes:
            - replace the main cities by their communal districts into a new
              S3GeoDataset object.
            - join this S3GeoDataset with the metadata to enrich it;
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
        geodataset should not be re-used after a call to this method.

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

        simplification = simplification if simplification else 0

        composite_geodataset = self.substitute_muncipal_districts(
            format_output=format_output
        )

        return composite_geodataset.create_downstream_geodatasets(
            metadata=metadata,
            format_output=format_output,
            niveau_polygons="ARRONDISSEMENT_MUNICIPAL",
            niveau_agreg=niveau_agreg,
            crs=crs,
            simplification=simplification,
            dict_corresp=dict_corresp,
        )


def concat_s3geodataset(
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
                dset.to_mercator(format_output=vectorfile_format)
                shutil.copytree(
                    dset.local_dir + "/preprocessed", f"{tempdir}/{k}"
                )

        output_path = mapshaper_concat(
            input_dir=f"{tempdir}/**/*",
            input_format=vectorfile_format,
            output_dir=f"{tempdir}/preprocessed_combined",
            output_name="COMMUNE",
            output_format=vectorfile_format,
        )
        logging.info("new S3GeoDataset created at %s", output_path)

        new_dset = S3GeoDataset(
            fs,
            local_dir=f"{tempdir}/preprocessed_combined",
            vectorfile_format=vectorfile_format,
            **config_new_dset,
        )
        new_dset.to_s3()

        return new_dset