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
export DARSHAN_LOG_DIR="/work/hdd/bdau/mbanisharifdehkordi/GNN_4_IO_5/darshan_log/darshan_log_E2E"
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

# Capture LUSTRE info BEFORE the run
echo "=== LUSTRE Configuration BEFORE Run ===" > lustre_info_${SLURM_JOB_ID}.txt
echo "Directory stripe settings:" >> lustre_info_${SLURM_JOB_ID}.txt
lfs getstripe . >> lustre_info_${SLURM_JOB_ID}.txt

echo "=== ULTRA-OPTIMIZED WITH DARSHAN ==="
echo "Optimizations: No fill + Large cache + Perfect stripes + Cubic layout"
echo "Start time: $(date)"

# Perfect cubic layout: 4x4x4 processes, 64x64x64 data per process
# Run with Darshan preloaded
LD_PRELOAD="$LIBDARSHAN" mpirun -np 64 ../write_3d_nc4_ultra_optimized optimized_4_4_4_64_64_64 4 4 4 64 64 64

echo "End time: $(date)"
ls -lah optimized_4_4_4_64_64_64.nc4

# Capture LUSTRE info AFTER the run for the actual data file
echo "" >> lustre_info_${SLURM_JOB_ID}.txt
echo "=== LUSTRE Configuration for Data File ===" >> lustre_info_${SLURM_JOB_ID}.txt
lfs getstripe optimized_4_4_4_64_64_64.nc4 >> lustre_info_${SLURM_JOB_ID}.txt

# Extract stripe count and size for easy reference
STRIPE_COUNT=$(lfs getstripe -c optimized_4_4_4_64_64_64.nc4)
STRIPE_SIZE=$(lfs getstripe -S optimized_4_4_4_64_64_64.nc4)
echo "" >> lustre_info_${SLURM_JOB_ID}.txt
echo "Summary: stripe_count=$STRIPE_COUNT, stripe_size=$STRIPE_SIZE" >> lustre_info_${SLURM_JOB_ID}.txt

# Find and save the Darshan log
echo "Looking for Darshan log..."
DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -name "*write_3d_nc4_ultra_optimized*" -type f -mmin -2 2>/dev/null | head -1)

if [ -z "$DARSHAN_FILE" ]; then
    DARSHAN_FILE=$(find $DARSHAN_LOG_DIR -type f -name "*.darshan" -mmin -2 2>/dev/null | head -1)
fi

if [ -n "$DARSHAN_FILE" ]; then
    # Create meaningful names for logs
    FINAL_LOG="$DARSHAN_LOG_DIR/e2e_ultra_optimized_${SLURM_JOB_ID}_64procs_32stripes_32mb.darshan"
    cp "$DARSHAN_FILE" "$FINAL_LOG"
    echo "Darshan log saved as: $FINAL_LOG"
    
    # Parse and verify the log (suppress LUSTRE errors)
    echo ""
    echo "=== Darshan Log Summary ==="
    darshan-parser "$FINAL_LOG" 2>/dev/null | head -50
    
    # Extract key metrics
    echo ""
    echo "=== Key I/O Performance Metrics ==="
    darshan-parser "$FINAL_LOG" 2>/dev/null | grep -E "total_bytes|agg_perf_by_slowest|POSIX_SIZE_WRITE|POSIX_FILE_ALIGNMENT|POSIX_CONSEC_WRITES|LUSTRE"
    
    # Save parsed output
    darshan-parser "$FINAL_LOG" 2>/dev/null > "${FINAL_LOG%.darshan}_parsed.txt"
    echo "Parsed output saved to: ${FINAL_LOG%.darshan}_parsed.txt"
    
    # Copy LUSTRE info to the same directory as Darshan log
    cp lustre_info_${SLURM_JOB_ID}.txt "$DARSHAN_LOG_DIR/e2e_ultra_optimized_${SLURM_JOB_ID}_lustre.txt"
    echo "LUSTRE info saved as: $DARSHAN_LOG_DIR/e2e_ultra_optimized_${SLURM_JOB_ID}_lustre.txt"
    
    # Create a combined metadata file for your analysis
    echo "Creating combined metadata file..."
    {
        echo "=== JOB METADATA ==="
        echo "JobID: ${SLURM_JOB_ID}"
        echo "Nodes: 2"
        echo "Tasks: 64"
        echo "Stripe Count: $STRIPE_COUNT"
        echo "Stripe Size: $STRIPE_SIZE"
        echo ""
        cat lustre_info_${SLURM_JOB_ID}.txt
    } > "$DARSHAN_LOG_DIR/e2e_ultra_optimized_${SLURM_JOB_ID}_metadata.txt"
    echo "Combined metadata saved as: $DARSHAN_LOG_DIR/e2e_ultra_optimized_${SLURM_JOB_ID}_metadata.txt"
    
    # Performance comparison section
    echo ""
    echo "=== Performance Comparison Metrics ==="
    echo "Configuration: Ultra-Optimized (32 stripes, 32MB stripe size, cubic layout)"
    darshan-parser "$FINAL_LOG" 2>/dev/null | grep -E "# run time|POSIX_BYTES_WRITTEN|POSIX_WRITES:|slowest_rank_io_time"
else
    echo "WARNING: No Darshan log found!"
fi