from dotenv import load_dotenv
import os
import s3fs

load_dotenv()

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "diffusion/shapefiles-test4"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BASE_CACHE_PATTERN = os.path.join("**", "*DONNEES_LIVRAISON*", "**")

kwargs = {}
for key in ["token", "secret", "key"]:
    try:
        kwargs[key] = os.environ[key]
    except KeyError:
        continue
FS = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL}, **kwargs)

from cartiflette.utils import *
from cartiflette.download import *
from cartiflette.s3 import *
