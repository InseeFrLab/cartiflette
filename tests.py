import os
import pandas as pd
os.chdir("cartogether/")

import dev
levels_year = [[y, dev.get_administrative_level_available_ign(year=y, verbose=False)] for y in range(2017,2023)]

list_levels = pd.DataFrame(levels_year, columns=['year','levels'])
list_levels = list_levels.explode("levels")
list_levels.groupby("levels").count()