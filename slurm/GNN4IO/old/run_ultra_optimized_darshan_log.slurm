#!/bin/bash
#SBATCH --job-name=e2e_ultra_opt_darshan
#SBATCH --nodes=2
#SBATCH --ntasks=64
#SBATCH --time=00:30:00
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --output=ultra_opt_darshan_%j.out
#SBATCH --error=ultra_opt_darshan_%j.err

# Load modules
module load gcc/11.4.0 openmpi/4.1.6 hdf5/1.14.3 netcdf-c/4.9.2

# Darshan setup
LIBDARSHAN="$HOME/darshan-patched-install/lib/libdarshan.so"

# Set Darshan environment variables
export DARSHAN_LOG_DIR="/work/hdd/bdau/mbanisharifdehkordi/E2E/darshan_logs"
export DARSHAN_LOGPATH="$DARSHAN_LOG_DIR"
export DARSHAN_LOGDIR="$DARSHAN_LOG_DIR"
export DARSHAN_LOG_PATH="$DARSHAN_LOG_DIR"
export DARSHAN_DISABLE_SHARED_REDUCTION=1
export DXT_ENABLE_IO_TRACE=1

# Create the log directory with date structure
mkdir -p "$DARSHAN_LOG_DIR/$(date +%Y)/$(date +%-m)/$(date +%-d)"

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_ultra_opt
cd results_ultra_opt

# Best case: 32 stripes, 32MB stripe size
lfs setstripe . -c 32 -S 32M

echo "=== ULTRA-OPTIMIZED WITH DARSHAN ==="
echo "Optimizations: No fill + Large cache + Perfect stripes + Cubic layout"
echo "Start time: $(date)"

# Perfect cubic layout: 4x4x4 processes, 64x64x64 data per process
# Run with Darshan preloaded
LD_PRELOAD="$LIBDARSHAN" mpirun -np 64 ../write_3d_nc4_ultra_optimized optimized_4_4_4_64_64_64 4 4 4 64 64 64

echo "End time: $(date)"
ls -lah optimized_4_4_4_64_64_64.nc4

# Find and save the Darshan log
echo "Looking for Darshan log..."
DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -name "*write_3d_nc4_ultra_optimized*" -type f -mmin -2 2>/dev/null | head -1)

# If not found by name, look for any recent log
if [ -z "$DARSHAN_FILE" ]; then
    DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -type f -name "*.darshan" -mmin -2 2>/dev/null | head -1)
fi

if [ -n "$DARSHAN_FILE" ]; then
    # Create a meaningful name for the log
    FINAL_LOG="$DARSHAN_LOG_DIR/e2e_ultra_optimized_${SLURM_JOB_ID}_64procs_32stripes_32mb.darshan"
    cp "$DARSHAN_FILE" "$FINAL_LOG"  # Use cp to preserve original
    echo "Darshan log saved as: $FINAL_LOG"
    
    # Parse and verify the log
    echo ""
    echo "=== Darshan Log Summary ==="
    darshan-parser "$FINAL_LOG" | head -50
    
    # Extract key metrics for optimized case analysis
    echo ""
    echo "=== Key I/O Performance Metrics ==="
    darshan-parser "$FINAL_LOG" | grep -E "total_bytes|agg_perf_by_slowest|POSIX_SIZE_WRITE|POSIX_FILE_ALIGNMENT|POSIX_CONSEC_WRITES|collective"
    
    # Extract Lustre striping information
    echo ""
    echo "=== Lustre Stripe Configuration ==="
    darshan-parser "$FINAL_LOG" | grep -E "LUSTRE_STRIPE_WIDTH|LUSTRE_STRIPE_SIZE|LUSTRE_OST_ID"
    
    # Save parsed output for comparison with pathological case
    darshan-parser "$FINAL_LOG" > "${FINAL_LOG%.darshan}_parsed.txt"
    echo "Parsed output saved to: ${FINAL_LOG%.darshan}_parsed.txt"
    
    # Generate comparison-ready metrics
    echo ""
    echo "=== Performance Comparison Metrics ==="
    echo "Configuration: Ultra-Optimized (32 stripes, 32MB stripe size, cubic layout)"
    darshan-parser "$FINAL_LOG" | grep -E "# run time|POSIX_BYTES_WRITTEN|POSIX_WRITES:|slowest_rank_io_time"
else
    echo "WARNING: No Darshan log found!"
    echo "Checking all recent files in $DARSHAN_LOG_DIR:"
    find $DARSHAN_LOG_DIR -type f -mmin -5 -ls
fi