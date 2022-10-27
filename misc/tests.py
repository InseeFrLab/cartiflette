import os
import pandas as pd
os.chdir("cartiflette/")

from cartiflette.download import get_vectorfile_ign

france = get_vectorfile_ign(
    level = "COMMUNE", field = "metropole",
    source = "COG", provider="opendatarchives").to_crs(2154)

### OLD

import dev
levels_year = [[y, dev.get_administrative_level_available_ign(year=y, verbose=False)] for y in range(2017,2023)]

list_levels = pd.DataFrame(levels_year, columns=['year','levels'])
list_levels = list_levels.explode("levels")
list_levels.groupby("levels").count()


france = dev.get_vectorfile_ign(level = "COMMUNE", field = "metropole").to_crs(2154)
martinique = dev.get_vectorfile_ign(level = "COMMUNE", field = "martinique")
martinique_mod = martinique
martinique_mod.crs = 2154
martinique_mod['trans_geometry'] = martinique_mod.scale(xfact=1.85, yfact=1.85, origin = (0,0)).translate(-1134525, 3517169)
martinique_mod = martinique_mod.set_geometry("trans_geometry", crs = 2154).drop("geometry", axis = "columns")

reunion = dev.get_vectorfile_ign(level = "COMMUNE", field = "reunion")
reunion_mod = reunion
reunion_mod.crs = 2154
reunion_mod['trans_geometry'] = reunion_mod.scale(xfact=1.75, yfact=1.75, origin = (0,0)).to_crs(2154).translate(-422169,-7132230)
reunion_mod = reunion_mod.set_geometry("trans_geometry", crs = 2154).drop("geometry", axis = "columns")


guadeloupe = dev.get_vectorfile_ign(level = "COMMUNE", field = "guadeloupe")
guadeloupe = guadeloupe
guadeloupe.crs = 2154
guadeloupe['trans_geometry'] = guadeloupe.scale(xfact=1.32, yfact=1.32, origin = (0,0)).to_crs(2154).translate(-699983,4269050)
guadeloupe = guadeloupe.set_geometry("trans_geometry", crs = 2154).drop("geometry", axis = "columns")

guyane = dev.get_vectorfile_ign(level = "COMMUNE", field = "guyane")
guyane = guyane
guyane.crs = 2154
guyane['trans_geometry'] = guyane.scale(xfact=0.25, yfact=0.25, origin = (0,0)).to_crs(2154).translate(118687,6286270)
guyane = guyane.set_geometry("trans_geometry", crs = 2154).drop("geometry", axis = "columns")

zoom = france.loc[france['INSEE_DEP'].isin([75,92,93,94])]
zoom['trans_geometry'] = zoom.scale(xfact=2.78, yfact=2.78, origin = (0,0)).to_crs(2154).translate(-1634506,-12046235)
zoom = zoom.set_geometry("trans_geometry", crs = 2154).drop("geometry", axis = "columns")
