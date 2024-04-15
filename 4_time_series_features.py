# %% xr_fresh env

# convert multiband images to single band
from glob import glob
import os
import re
import geowombat as gw
from numpy import int16
from tqdm import tqdm
from multiprocessing import Pool

os.chdir(r"/mnt/bigdrive/Dropbox/wb_malawi/mosaic")
images = glob("*.tif")
images
# %%


def write_bands(image):
    with gw.open(image) as src:
        for i in range(len(src)):
            # make folder for each band
            os.makedirs(f"../single_band_mosaics/B{i}", exist_ok=True)

            # write single band images
            gw.save(
                src[i].astype(int16),
                compress="LZW",
                filename=f"../single_band_mosaics/B{i}/{os.path.basename(image).split('.')[0]}"
                + f"_B{i}.tif",
                num_workers=1,
                overwrite=True,
            )


# map write_bands function

with Pool(10) as p:
    # Use tqdm for progress bar
    for _ in tqdm(p.imap_unordered(write_bands, images), total=len(images)):
        pass
# %%