import requests
import os
import py7zr

url = "https://wxs.ign.fr/x02uy2aiwjo9bm8ce5plwqmr/telechargement/prepackage/ADMINEXPRESS-COG-CARTO_SHP_TERRITOIRES_PACK_2023-05-04$ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03/file/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03.7z"

response = requests.get(url, stream = True)

with open("data.7z", "wb") as f:
    f.write(response.content)


def file_size(file_name):
    file_stats = os.stat(file_name)
    return f'File Size in MegaBytes is {file_stats.st_size / (1024 * 1024)}'

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

with py7zr.SevenZipFile('data.7z', mode='r') as z:
    z.extractall()


file_size("data.7z")
get_size("ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03") / (1024 * 1024)



file_size("out.topojson")
