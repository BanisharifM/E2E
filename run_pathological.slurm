#!/bin/bash
#SBATCH --job-name=e2e_pathological
#SBATCH --nodes=2
#SBATCH --ntasks=64
#SBATCH --time=00:30:00
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --output=pathological_%j.out
#SBATCH --error=pathological_%j.err

module load gcc/11.4.0 openmpi/4.1.6 hdf5/1.14.3 netcdf-c/4.9.2

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_pathological
cd results_pathological

# Worst case: 1 stripe, 64KB stripe size
lfs setstripe . -c 1 -S 65536

echo "=== PATHOLOGICAL BASELINE ==="
echo "Bottlenecks: Fill + Sync after each write + Bad stripe + Bad layout"
echo "Start time: $(date)"

# Bad layout: 64x1x1 processes, 2x2x2048 data per process (thin tall blocks)
time mpirun -np 64 ../write_3d_nc4_pathological pathological_64_1_1_16_16_16 64 1 1 16 16 16

echo "End time: $(date)"
ls -lah pathological_64_1_1_2_2_2048.nc4
