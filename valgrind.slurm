#!/bin/bash
#SBATCH --job-name="asg1-pain"
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

module purge

module load foss/2022a

valgrind --leak-check=full --show-leak-kinds=all ./build/main ./data/twitter-1mb.json

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -v