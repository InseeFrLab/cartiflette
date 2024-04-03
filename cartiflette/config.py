# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
import s3fs

load_dotenv()

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "production"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

kwargs = {}
for key in ["token", "secret", "key"]:
    try:
        kwargs[key] = os.environ[key]
    except KeyError:
        continue
FS = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL}, **kwargs)

THREADS_DOWNLOAD = 5
# Nota : each thread may also span the same number of children threads;
# set to 1 for debugging purposes (will deactivate multithreading)

LEAVE_TQDM = False
