#!/bin/bash
#SBATCH --job-name=e2e_optimized_nofill
#SBATCH --nodes=2
#SBATCH --ntasks=64
#SBATCH --time=00:30:00
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --output=optimized_nofill_%j.out
#SBATCH --error=optimized_nofill_%j.err

module load gcc/11.4.0 openmpi/4.1.6 hdf5/1.14.3 netcdf-c/4.9.2

export NETCDF_DIR=/sw/spack/deltas11-2023-03/apps/linux-rhel8-zen3/gcc-11.4.0/netcdf-c-4.9.2-pzwctxp
export HDF5_DIR=/sw/spack/deltas11-2023-03/apps/linux-rhel8-zen3/gcc-11.4.0/hdf5-1.14.3-7b3feas

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_nofill
cd results_nofill

# Optimized configuration: 4 stripes, 4MB stripe size
lfs setstripe . -c 4 -S 4M

echo "=== E2E Optimized Test WITHOUT FILL VALUES ==="
echo "Configuration: 64 processes, 4x4x4 grid, optimized data layout"
echo "Fill values: DISABLED (paper's optimization)"
echo "Start time: $(date)"

# Run optimized test without fill values
time mpirun -np 64 ../write_3d_nc4_optimized optimized_nofill_4_4_4_256_16_8 4 4 4 256 16 8

echo "End time: $(date)"
ls -lah optimized_nofill_4_4_4_256_16_8.nc4
lfs getstripe optimized_nofill_4_4_4_256_16_8.nc4
