# -*- coding: utf-8 -*-
from charset_normalizer import from_bytes, is_binary
import fiona
import geopandas as gpd
import logging
import os
from shapely.geometry import box

from cartiflette.download.dataset import Dataset
from cartiflette.constants import REFERENCES

logger = logging.getLogger(__name__)


class Layer:
    def __init__(self, dataset: Dataset, cluster_name: str, files: dict):
        """
        Layer present in a dataset. A layer is defined by a distinctive
        combination of path and basename (without extension). To that effect,
        each auxialary file associated to a shapefile shall be present in the
        same layer.

        Nota : distinction between selected and unselected files in `files`
        argument helps to evaluate territory using a shapefile even if the
        only file targeted is a dbf.

        Parameters
        ----------
        dataset : Dataset
            Dataset containing layers
        cluster_name : str
            Unique name for a layer (computed by the scraper after data
            unpacking) and corresponding to the minimum recursive distinct path
            differencing each layer of the dataset.
        files : dict
            Dict of files present in the layer, wether they should be uploaded
            or not. The dictionnary consist of pairs of files information:
            {file_path: str -> should be uploaded: bool}
        """

        self.dataset = dataset
        self.dataset_family = dataset.dataset_family
        self.source = dataset.source
        self.year = dataset.year
        self.territory = dataset.territory
        self.provider = dataset.provider
        self.cluster_name = cluster_name
        self.files = files

        self.files_to_upload = {
            path: cluster_name + os.path.splitext(path)[-1]
            for path, to_upload in files.items()
            if to_upload
        }
        self._get_format()
        self._gis_and_encoding_evaluation()

    def __str__(self):
        name = f"<Layer {self.cluster_name} from {self.dataset}>"
        return name

    def __repr__(self):
        return self.__str__()

    def _get_format(self):
        if any(x.lower().split(".")[-1] == "shp" for x in self.files_to_upload):
            self.format = "shp"
        else:
            # assume there is only one file
            self.format = list(self.files_to_upload)[0].split(".")[-1]

    def _get_encoding(self):
        ref_cpg_file = [x for x in self.files if x.lower().split(".")[-1] == "cpg"]
        try:
            ref_cpg_file = ref_cpg_file[0]
        except IndexError:
            return None
        with open(ref_cpg_file, "r") as f:
            encoding = f.read()

        return encoding.lower()

    def _get_gis_file(self):
        ref_gis_file = [x for x in self.files if x.lower().split(".")[-1] == "shp"]
        try:
            ref_gis_file = ref_gis_file[0]
        except IndexError:
            # assume there is only one file (and test it later)
            ref_gis_file = list(self.files)[0]

        return ref_gis_file

    def _gis_and_encoding_evaluation(self):
        encoding = self._get_encoding()
        kwargs = {"encoding": encoding} if encoding else {}
        ref_gis_file = self._get_gis_file()
        try:
            # Note : read all rows to evaluate bbox / territory
            gdf = gpd.read_file(ref_gis_file, **kwargs)
            self.crs = gdf.crs.to_epsg()

            if not self.crs:
                logger.warning(
                    f"{self} - projection without known EPSG, "
                    "layer will be reprojected to 4326"
                )

                # Let's reproject...
                gdf = gdf.to_crs(4326)
                self.crs = 4326

                # let's overwrite initial files
                gdf.to_file(ref_gis_file, encoding="utf-8")

            elif encoding and encoding != "utf-8":
                logger.warning(
                    f"{self} - encoding={encoding}, " "layer will be re-encoded to UTF8"
                )
                # let's overwrite initial files with utf8...
                gdf.to_file(ref_gis_file, encoding="utf-8")

        except (AttributeError, fiona.errors.DriverError):
            # Non-native-GIS dataset
            self.crs = None

        if self.crs:
            bbox = box(*gdf.total_bounds)
            bbox = gpd.GeoSeries([bbox], crs=gdf.crs)

            intersects = REFERENCES.sjoin(
                bbox.to_frame().to_crs(REFERENCES.crs),
                how="left",
                predicate="intersects",
            )
            intersects = set(intersects.dropna().location)

            if len(intersects) == 1:
                self.territory = intersects.pop()
            elif len(intersects) > 1 and "metropole" in intersects:
                self.territory = "france_entiere"
            else:
                logger.warning(
                    f"{self} : spatial join used for territory recognition "
                    "failed, dataset's raw description will be used instead"
                )
                self.territory = self.dataset.territory

        elif not self.crs:
            # TODO : chercher un champ de clefs INSEE ?
            logger.info(
                f"{self} : coverage analysis of non-gis files is not yet "
                "implemented, dataset's raw description will be used instead"
            )
            self.territory = self.dataset.territory

            for file in list(self.files):
                if not is_binary(file):
                    # attempt to detect encoding
                    with open(file, "rb") as f:
                        data = f.read()
                    try:
                        best = from_bytes(
                            data,
                            cp_isolation=[
                                "utf-8",
                                "1252",
                                "iso8859_16",
                                "iso8859_15",
                                "iso8859_1",
                                "ibm_1252",
                            ],
                        ).best()
                        encoding = best.encoding
                    except Exception:
                        pass
                    else:
                        if encoding != "utf_8":
                            logger.warning(
                                f"{self} - encoding={encoding}, "
                                "layer will be re-encoded to UTF8"
                            )
                            with open(file, "w", encoding="utf8"):
                                data.decode(encoding)
