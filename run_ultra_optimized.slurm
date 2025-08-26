#!/bin/bash
#SBATCH --job-name=e2e_ultra_opt
#SBATCH --nodes=2
#SBATCH --ntasks=64
#SBATCH --time=00:30:00
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --output=ultra_opt_%j.out
#SBATCH --error=ultra_opt_%j.err

module load gcc/11.4.0 openmpi/4.1.6 hdf5/1.14.3 netcdf-c/4.9.2

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_ultra_opt
cd results_ultra_opt

# Best case: 32 stripes, 32MB stripe size
lfs setstripe . -c 32 -S 32M

echo "=== ULTRA-OPTIMIZED ==="
echo "Optimizations: No fill + Large cache + Perfect stripes + Cubic layout"
echo "Start time: $(date)"

# Perfect cubic layout: 4x4x4 processes, 128x128x128 data
time mpirun -np 64 ../write_3d_nc4_ultra_optimized ultra_opt_4_4_4_128_128_128 4 4 4 128 128 128

echo "End time: $(date)"
ls -lah ultra_opt_4_4_4_128_128_128.nc4
