import os
from cartiflette.download import store_vectorfile_ign

path = store_vectorfile_ign(
    source="EXPRESS-COG-TERRITOIRE", year=2022, field="metropole"
)
path = store_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2021, field="reunion")
path = store_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2020, field="reunion")
path = store_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2019, field="reunion")

for c in ["metropole", "reunion", "guadeloupe", "martinique", "mayotte", "guyane"]:
    path = store_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2022, field=c)
    print(os.listdir(path))

for c in ["metropole", "reunion", "guadeloupe", "martinique", "mayotte", "guyane"]:
    path = store_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2021, field=c)
    print(os.listdir(path))

for c in ["metropole", "reunion", "guadeloupe", "martinique", "mayotte", "guyane"]:
    path = store_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2020, field=c)
    print(os.listdir(path))

for c in ["metropole", "reunion", "guadeloupe", "martinique", "mayotte", "guyane"]:
    path = store_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2019, field=c)
    print(os.listdir(path))


path = store_vectorfile_ign(source="EXPRESS-COG", year=2022, field="metropole")
path = store_vectorfile_ign(source="EXPRESS-COG", year=2021, field="metropole")
path = store_vectorfile_ign(source="EXPRESS-COG", year=2020, field="reunion")
path = store_vectorfile_ign(source="EXPRESS-COG", year=2019, field="reunion")
