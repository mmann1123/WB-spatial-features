# %%
import geowombat as gw
from glob import glob
import os
import re

os.chdir("/mnt/bigdrive/Dropbox/wb_malawi")
south_tiles = sorted(glob("./tiles/S2_SR_*_south*.tif"))
south_tiles
# %%
# Get unique grid codes
pattern = r"(?<=-)\d+-\d+(?=\.tif)"
unique_grids = list(
    set(
        [
            re.search(pattern, file_path).group()
            for file_path in south_tiles
            if re.search(pattern, file_path)
        ]
    )
)
unique_grids
# %% interpolate missing
from xr_fresh.interpolate_series import interpolate_nan
from numpy import nan
!mkdir -p interpolated

missing_data = nan

# Print the unique codes
for grid in unique_grids:
    print("working on grid", grid)
    a_grid = sorted([f for f in south_tiles if grid in f])
    print('files:' , a_grid)
    with gw.series(
        a_grid,
        transfer_lib="numpy",
        window_size=[512, 512],
    ) as src:
        src.apply(
            func=interpolate_nan(
                interp_type="linear",
                missing_value=missing_data,
                count=len(src.filenames),
            ),
            outfile=os.path.join(
            os.getcwd(),
            "interpolated",
            f"S2_SR_linear_interp_{grid}.tif",
        ),
            num_workers=1,  # src.nchunks,
            bands=1,
            kwargs={"BIGTIFF": "YES"},
        )

# %%
