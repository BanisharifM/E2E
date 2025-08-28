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

# Use the fresh Darshan build
LIBDARSHAN="$HOME/darshan-fresh/lib/libdarshan.so"

# Set Darshan environment variables
export DARSHAN_LOG_DIR="/work/hdd/bdau/mbanisharifdehkordi/E2E/darshan_logs"
export DARSHAN_LOGPATH="$DARSHAN_LOG_DIR"
export DARSHAN_LOGDIR="$DARSHAN_LOG_DIR"
export DARSHAN_LOG_PATH="$DARSHAN_LOG_DIR"
export DARSHAN_DISABLE_SHARED_REDUCTION=1
export DXT_ENABLE_IO_TRACE=1

# Create the log directory with date structure (Darshan expects this)
mkdir -p "$DARSHAN_LOG_DIR/$(date +%Y)/$(date +%-m)/$(date +%-d)"

cd /work/hdd/bdau/mbanisharifdehkordi/E2E/3d
mkdir -p results_pathological
cd results_pathological

# Worst case: 1 stripe, 64KB stripe size
lfs setstripe . -c 1 -S 65536

echo "=== PATHOLOGICAL BASELINE WITH DARSHAN ==="
echo "Bottlenecks: Fill + Sync after each write + Bad stripe + Bad layout"
echo "Start time: $(date)"

# Run with Darshan preloaded and time measurement
LD_PRELOAD="$LIBDARSHAN" time mpirun -np 64 ../write_3d_nc4_pathological pathological_64_1_1_16_16_16 64 1 1 16 16 16

echo "End time: $(date)"
ls -lah pathological_64_1_1_16_16_16.nc4

# Find the Darshan log (it will be in the dated subdirectory)
echo "Looking for Darshan log..."
DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -name "*write_3d_nc4_pathological*" -type f -mmin -2 2>/dev/null | head -1)

if [ -z "$DARSHAN_FILE" ]; then
    DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -type f -name "*.darshan" -mmin -2 2>/dev/null | head -1)
fi

if [ -n "$DARSHAN_FILE" ]; then
    # Copy to main directory with meaningful name
    FINAL_LOG="$DARSHAN_LOG_DIR/e2e_pathological_${SLURM_JOB_ID}_64procs_1stripe_64kb.darshan"
    cp "$DARSHAN_FILE" "$FINAL_LOG"
    echo "Darshan log saved as: $FINAL_LOG"
    
    # Parse and verify the log with the new darshan-parser
    echo ""
    echo "=== Darshan Log Summary ==="
    $HOME/darshan-fresh/bin/darshan-parser "$FINAL_LOG" | head -50
    
    # Extract key metrics including LUSTRE data
    echo ""
    echo "=== Key I/O Performance Metrics ==="
    $HOME/darshan-fresh/bin/darshan-parser "$FINAL_LOG" | grep -E "total_bytes|agg_perf_by_slowest|POSIX_SIZE_WRITE|POSIX_FILE_ALIGNMENT|POSIX_CONSEC_WRITES"
    
    echo ""
    echo "=== LUSTRE Configuration ==="
    $HOME/darshan-fresh/bin/darshan-parser "$FINAL_LOG" | grep -A10 "LUSTRE module data"
    
    # Save complete parsed output
    $HOME/darshan-fresh/bin/darshan-parser "$FINAL_LOG" > "${FINAL_LOG%.darshan}_parsed.txt"
    echo "Parsed output saved to: ${FINAL_LOG%.darshan}_parsed.txt"
    
    # Clean up the dated subdirectory log if copy was successful
    if [ -f "$FINAL_LOG" ]; then
        rm "$DARSHAN_FILE"
        echo "Original log removed from dated subdirectory"
    fi
else
    echo "WARNING: No Darshan log found!"
    echo "Checking all recent files in $DARSHAN_LOG_DIR:"
    find $DARSHAN_LOG_DIR -type f -mmin -5 -ls
fi
