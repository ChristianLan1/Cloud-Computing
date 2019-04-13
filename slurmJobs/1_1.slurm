#!/bin/bash
#SBATCH -p cloud
#SBATCH --output 1n1c1task.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=0-01:00:00

# Load required modules
module load Python/3.6.1-intel-2017.u2
# Launch multiple process python code
time mpiexec -n 1 python3 Assignment1.py -i /data/projects/COMP90024/bigTweet.json

