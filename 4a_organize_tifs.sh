#!/bin/bash
#SBATCH -p short
#SBATCH -J write_tifs
#SBATCH --export=NONE
#SBATCH -t 1-00:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=mmann1123@gwu.edu
#SBATCH -e write_tifs.err
#SBATCH -o write_tifs.out

# Use this to run 4_organize_tifs.py 

source  ~/miniforge3/envs/geowombat/bin/python
pip install pendulum 
python 4_organize_tifs.py 