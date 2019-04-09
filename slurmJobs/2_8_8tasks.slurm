#!/bin/bash
#SBATCH -p physical
#SBATCH --output 2n8c8tasks.txt
#SBATCH --nodes=2

#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1

#SBATCH --time=0-00:15:00



# Load required modules

module load Python/3.6.1-intel-2017.u2



# Launch multiple process python code


time mpirun python3 Assignment1.py -i /data/projects/COMP90024/bigTweet.json

