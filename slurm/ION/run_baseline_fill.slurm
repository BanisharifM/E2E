#!/bin/bash
#SBATCH --job-name=e2e_baseline_fill
#SBATCH --nodes=2
#SBATCH --ntasks=64
#SBATCH --time=00:30:00
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --output=baseline_fill_%j.out
#SBATCH --error=baseline_fill_%j.err

module load gcc/11.4.0 openmpi/4.1.6 hdf5/1.14.3 netcdf-c/4.9.2

export NETCDF_DIR=/sw/spack/deltas11-2023-03/apps/linux-rhel8-zen3/gcc-11.4.0/netcdf-c-4.9.2-pzwctxp
export HDF5_DIR=/sw/spack/deltas11-2023-03/apps/linux-rhel8-zen3/gcc-11.4.0/hdf5-1.14.3-7b3feas

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_fill
cd results_fill

# Baseline configuration: 1 stripe, 1MB stripe size
lfs setstripe . -c 1 -S 1M

echo "=== E2E Baseline Test WITH FILL VALUES (should be slower) ==="
echo "Configuration: 64 processes, 4x4x4 grid, 32x32x16 data per process"
echo "Fill values: ENABLED (reproducing paper's bottleneck)"
echo "Start time: $(date)"

# Run baseline test with fill values
time mpirun -np 64 ../write_3d_nc4_baseline baseline_fill_4_4_4_32_32_16 4 4 4 32 32 16

echo "End time: $(date)"
ls -lah baseline_fill_4_4_4_32_32_16.nc4
lfs getstripe baseline_fill_4_4_4_32_32_16.nc4
