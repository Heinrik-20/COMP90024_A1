#!/bin/bash
#SBATCH --job-name="asg1-pain"
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

module purge

module load GCC/11.3.0
module load OpenMPI/4.1.4


srun -n 4 ./build/main ./data/twitter-1mb.json

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n