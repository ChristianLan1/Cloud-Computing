#!/bin/bash
#SBATCH -p cloud
#SBATCH --output 1node8core.txt
#SBATCH --nodes=1

#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1

#SBATCH --time=0-00:10:00



# Load required modules

module load Python/3.6.1-intel-2017.u2



# Launch multiple process python code



time mpiexec -n 8 python3 Assignment1.py -i /data/projects/COMP90024/bigTweet.json

