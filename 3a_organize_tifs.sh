#!/bin/bash
#SBATCH -p short
#SBATCH -J write_tifs
#SBATCH --export=NONE
#SBATCH -t 1-00:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=mmann1123@gwu.edu
#SBATCH -e write_tifs.err
#SBATCH -o write_tifs.out


export PATH="/groups/engstromgrp/anaconda3/bin:$PATH"
source activate geowombat

python 3a_organize_tifs.py 