from __future__ import annotations

import os
import sys
import json
import time
import multiprocessing
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional

# Allow running from root directory
if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from EWMRS.ingest.mrms.main import download_all_files
from EWMRS.ingest.wpc.main import run_wpc_ingest
from EWMRS.render.tools import TransformUtils
from EWMRS.render.render import GUILayerRenderer
from EWMRS.render.config import file_list
from EWMRS.util import file as fs
from EWMRS.util.io import IOManager, TimestampedOutput, QueueWriter
from EWMRS.scheduler import MRMSUpdateChecker
from EWMRS.ingest.mrms.config import get_check_modifiers
check_modifiers = get_check_modifiers()

# Timestamp outputs globally
sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Pipeline]")


def _ensure_dt(dt_in) -> datetime:
    if isinstance(dt_in, datetime):
        dt = dt_in
    elif isinstance(dt_in, str):
        dt = datetime.fromisoformat(dt_in)
    else:
        raise TypeError("dt must be a datetime or ISO-format string")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _render_layer(layer):
    """Render a single layer. Returns (name, png_path or None).
    
    Module-level function for ProcessPoolExecutor compatibility.
    """
    from EWMRS.render.tools import TransformUtils
    from EWMRS.render.render import GUILayerRenderer
    from EWMRS.util.io import IOManager
    
    io_mgr = IOManager("[Pipeline]")
    
    name = layer.get("name")
    colormap_key = layer.get("colormap_key")
    src_dir = Path(layer.get("filepath"))
    out_dir = Path(layer.get("outdir"))

    io_mgr.write_debug(f"Processing layer {name}: src={src_dir}, out={out_dir}")

    try:
        if not src_dir.exists():
            io_mgr.write_warning(f"Source directory missing for {name}: {src_dir}")
            return name, None

        # Find most recent file by modification time
        candidate_files = [f for f in src_dir.glob("*") if f.is_file()]
        if not candidate_files:
            io_mgr.write_warning(f"No source files found for {name} in {src_dir}")
            return name, None

        # Optimization: Sort by filename instead of mtime.
        # MRMS files contain timestamps in the name (e.g. YYYYMMDD-HHMMSS).
        # Lexicographical sort is faster and equivalent to chronological sort for these files.
        latest_file = max(candidate_files, key=lambda f: f.name)

        io_mgr.write_info(f"Found latest file for {name}: {latest_file}")

        ds = TransformUtils.load_ds(latest_file)
        if ds is None:
            io_mgr.write_error(f"Failed to load dataset for {latest_file}")
            return name, None

        timestamp_iso = TransformUtils.find_timestamp(str(latest_file))

        renderer = GUILayerRenderer(ds, out_dir, colormap_key, name, timestamp_iso)
        png_path, px_timestamp = renderer.convert_to_png()

        return name, png_path

    except Exception as e:
        io_mgr.write_error(f"Error processing layer {name}: {e}")
        return name, None


def cleanup_old_gui_files(max_age_minutes: int = 120):
    """Remove PNG files older than max_age_minutes from GUI output directories.
    
    Also cleans up stale entries from index.json files.
    """
    import time
    
    now = time.time()
    max_age_seconds = max_age_minutes * 60
    total_removed = 0
    
    for layer in file_list:
        out_dir = Path(layer.get("outdir"))
        if not out_dir.exists():
            continue
        
        # Clean up old PNG files
        for png_file in out_dir.glob("*.png"):
            try:
                file_age = now - png_file.stat().st_mtime
                if file_age > max_age_seconds:
                    png_file.unlink()
                    total_removed += 1
            except Exception as e:
                io_manager.write_warning(f"Failed to remove {png_file}: {e}")
        
        # Update index.json to remove stale timestamps
        index_file = out_dir / "index.json"
        if index_file.exists():
            try:
                import json
                with open(index_file, 'r') as f:
                    timestamps = json.load(f)
                
                # Keep only timestamps that have corresponding PNG files
                existing_pngs = {p.stem.split('_')[-1] for p in out_dir.glob("*.png")}
                timestamps = [ts for ts in timestamps if ts in existing_pngs]
                
                with open(index_file, 'w') as f:
                    json.dump(timestamps, f)
            except Exception as e:
                io_manager.write_warning(f"Failed to update index.json in {out_dir}: {e}")
    
    if total_removed > 0:
        io_manager.write_info(f"Cleaned up {total_removed} old GUI files (>{max_age_minutes} min)")


def run_render_pipeline(dt, max_entries: int = 10, download: bool = True) -> Dict[str, Optional[Path]]:
    """Run render pipeline for configured layers at the specified datetime.

    This function optionally runs the ingest downloader first and then renders
    all layers configured in `render.config.file_list` using the newest file
    available in each source `filepath`.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    dt = _ensure_dt(dt)
    results: Dict[str, Optional[Path]] = {}

    if download:
        io_manager.write_info(f"Starting ingest downloads for dt={dt} (max_entries={max_entries})")
        try:
            download_all_files(dt, max_entries=max_entries)
        except Exception as e:
            io_manager.write_error(f"Download step failed: {e}")

    # Render layers in parallel using separate processes (true multi-core)
    io_manager.write_info(f"Rendering {len(file_list)} layers across 4 CPU cores...")
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_render_layer, layer): layer for layer in file_list}
        for future in as_completed(futures):
            name, png_path = future.result()
            results[name] = png_path

    # Clean up old GUI files (>120 min)
    cleanup_old_gui_files(max_age_minutes=120)

    return results


# ----------------- Scheduler-style loop (download + render only) -----------------

def pipeline(log_queue, dt, max_entries=10):
    """Run the simplified ingestion + render pipeline once, sending logs to `log_queue`."""
    # Redirect stdout/stderr to queue writer for the child process
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg: str):
        log_queue.put(str(msg))

    try:
        log(f"INFO: Starting Data Ingestion for timestamp {dt}")
        # Download files (blocking)
        try:
            download_all_files(dt, max_entries=max_entries)
            log("INFO: Download completed")
        except Exception as e:
            log(f"ERROR: Download failed - {e}")

        # Run WPC Ingest
        log("INFO: Starting WPC Ingest")
        try:
            run_wpc_ingest(log_queue)
        except Exception as e:
            log(f"ERROR: WPC Ingest failed - {e}")

        # Render using local files (download step above populates them)
        log("INFO: Starting Render step")
        try:
            results = run_render_pipeline(dt, max_entries=max_entries, download=False)
            log(f"INFO: Render completed: {results}")
        except Exception as e:
            log(f"ERROR: Render failed - {e}")

        log("INFO: Pipeline completed successfully")
    except Exception as e:
        log(f"ERROR: Pipeline failed - {e}")



# ----------------- WPC Ingest Process -----------------



def main(watch: bool = True, poll_interval: float = 15.0):
    """If `watch` is True, poll MRMS sources and WPC sources for new data.
    Spawns multiprocessing child processes to run pipelines when new data appears.
    """
    print("Scheduler started. Press CTRL+C to exit.")
    checker = MRMSUpdateChecker(verbose=True)
    last_processed = None

    # Load state
    state_file = fs.BASE_DIR / "latest_processed.json"
    if state_file.exists():
        try:
            with open(state_file, 'r') as f:
                data = json.load(f)
                if "last_processed" in data:
                    last_processed = _ensure_dt(data["last_processed"])
                    print(f"[Scheduler] Resuming from timestamp: {last_processed}")
        except Exception as e:
            print(f"[Scheduler] Failed to load state file: {e}")

    try:
        while True:
            now_ts = time.time()
            now = datetime.now(timezone.utc)
            
            # --- Check MRMS Updates ---
            latest_common = checker.latest_common_minute_1h(check_modifiers)

            if latest_common and latest_common != last_processed:
                print(f"[Scheduler] DEBUG: New latest common timestamp: {latest_common}")
                dt = latest_common
                last_processed = latest_common

                # Save state
                try:
                    with open(state_file, 'w') as f:
                        json.dump({"last_processed": last_processed.isoformat()}, f)
                except Exception as e:
                    print(f"[Scheduler] Failed to save state file: {e}")

                # Queue to capture logs from child process
                log_queue = multiprocessing.Queue()

                # Spawn pipeline as a separate process
                proc = multiprocessing.Process(target=pipeline, args=(log_queue, dt))
                proc.start()
                print(f"Spawned MRMS pipeline process PID={proc.pid}")

                # Relay logs in real-time
                while proc.is_alive() or not log_queue.empty():
                    while not log_queue.empty():
                        print(log_queue.get())
                    time.sleep(1)

                proc.join()
                print(f"MRMS pipeline process PID={proc.pid} finished")

            else:
                if not latest_common:
                    print("[Scheduler] WARN: No common timestamp available yet. Waiting ...")
                else:
                    print(f"[Scheduler] DEBUG: Timestamp {latest_common} already processed. Waiting ...")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)


if __name__ == "__main__":
    # Start the live scheduler loop by default (no CLI arguments required)
    try:
        print("Starting EWMRS live scheduler. Press CTRL+C to exit.")
        main(watch=True)
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
