#!/bin/bash
#SBATCH --job-name=e2e_pathological_darshan
#SBATCH --nodes=2
#SBATCH --ntasks=64
#SBATCH --time=00:30:00
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --output=pathological_darshan_%j.out
#SBATCH --error=pathological_darshan_%j.err

# Load modules
module load gcc/11.4.0 openmpi/4.1.6 hdf5/1.14.3 netcdf-c/4.9.2

# Darshan setup
LIBDARSHAN="$HOME/darshan-patched-install/lib/libdarshan.so"

# Set Darshan environment variables (following your IOR pattern)
export DARSHAN_LOG_DIR="/work/hdd/bdau/mbanisharifdehkordi/E2E/darshan_logs"
export DARSHAN_LOGPATH="$DARSHAN_LOG_DIR"
export DARSHAN_LOGDIR="$DARSHAN_LOG_DIR"
export DARSHAN_LOG_PATH="$DARSHAN_LOG_DIR"
export DARSHAN_DISABLE_SHARED_REDUCTION=1
export DXT_ENABLE_IO_TRACE=1

# Create the log directory with date structure
mkdir -p "$DARSHAN_LOG_DIR/$(date +%Y)/$(date +%-m)/$(date +%-d)"

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_pathological
cd results_pathological

# Worst case: 1 stripe, 64KB stripe size
lfs setstripe . -c 1 -S 65536

echo "=== PATHOLOGICAL BASELINE WITH DARSHAN ==="
echo "Bottlenecks: Fill + Sync after each write + Bad stripe + Bad layout"
echo "Start time: $(date)"

# Run with Darshan preloaded (since we compiled with Darshan, LD_PRELOAD will enhance it)
LD_PRELOAD="$LIBDARSHAN" mpirun -np 64 ../write_3d_nc4_pathological pathological_64_1_1_16_16_16 64 1 1 16 16 16

echo "End time: $(date)"
ls -lah pathological_64_1_1_16_16_16.nc4

# Find and save the Darshan log
echo "Looking for Darshan log..."
DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -name "*write_3d_nc4_pathological*" -type f -mmin -2 2>/dev/null | head -1)

# If not found by name, look for any recent log
if [ -z "$DARSHAN_FILE" ]; then
    DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -type f -name "*.darshan" -mmin -2 2>/dev/null | head -1)
fi

if [ -n "$DARSHAN_FILE" ]; then
    # Create a meaningful name for the log
    FINAL_LOG="$DARSHAN_LOG_DIR/e2e_pathological_${SLURM_JOB_ID}_64procs_1stripe_64kb.darshan"
    cp "$DARSHAN_FILE" "$FINAL_LOG"  # Use cp instead of mv to preserve original
    echo "Darshan log saved as: $FINAL_LOG"
    
    # Parse and verify the log
    echo ""
    echo "=== Darshan Log Summary ==="
    darshan-parser "$FINAL_LOG" | head -50
    
    # Extract key metrics for pathological case analysis
    echo ""
    echo "=== Key I/O Performance Metrics ==="
    darshan-parser "$FINAL_LOG" | grep -E "total_bytes|agg_perf_by_slowest|POSIX_SIZE_WRITE|POSIX_FILE_ALIGNMENT|POSIX_CONSEC_WRITES"
    
    # Save parsed output for later analysis
    darshan-parser "$FINAL_LOG" > "${FINAL_LOG%.darshan}_parsed.txt"
    echo "Parsed output saved to: ${FINAL_LOG%.darshan}_parsed.txt"
else
    echo "WARNING: No Darshan log found!"
    echo "Checking all recent files in $DARSHAN_LOG_DIR:"
    find $DARSHAN_LOG_DIR -type f -mmin -5 -ls
fi
