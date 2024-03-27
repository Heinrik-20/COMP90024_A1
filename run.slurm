#!/bin/bash
#SBATCH --job-name="asg1-pain"
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

module purge

module load foss/2022a

time srun -n 1 ./build/main ./data/twitter-100gb.json

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -v