#!/bin/bash
#SBATCH --job-name=e2e_optimized
#SBATCH --nodes=2
#SBATCH --ntasks=64
#SBATCH --time=00:30:00
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --output=optimized_%j.out
#SBATCH --error=optimized_%j.err

module load gcc/11.4.0 openmpi/4.1.6 hdf5/1.14.3 netcdf-c/4.9.2

export NETCDF_DIR=/sw/spack/deltas11-2023-03/apps/linux-rhel8-zen3/gcc-11.4.0/netcdf-c-4.9.2-pzwctxp
export HDF5_DIR=/sw/spack/deltas11-2023-03/apps/linux-rhel8-zen3/gcc-11.4.0/hdf5-1.14.3-7b3feas

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_optimized
cd results_optimized

# Optimized configuration: 4 stripes, 4MB stripe size
lfs setstripe . -c 4 -S 4M

echo "=== E2E Optimized Test (reproducing 482.22 MiB/s) ==="
echo "Configuration: 64 processes, 4x4x4 grid, optimized data layout"
echo "Start time: $(date)"

# Run optimized test (adjusted dimensions to match physical layout)
time mpirun -np 64 ../write_3d_nc4 optimized_4_4_4_256_16_8 4 4 4 256 16 8

echo "End time: $(date)"
ls -lah optimized_4_4_4_256_16_8.nc4
lfs getstripe optimized_4_4_4_256_16_8.nc4
