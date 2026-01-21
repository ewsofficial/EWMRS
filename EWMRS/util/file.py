from pathlib import Path
import sys
import os
import platform
from datetime import datetime

from ..util.io import IOManager

io_manager = IOManager("[Util]")

# ---------- BASE DIRECTORY CONFIGURATION ----------
# Default base directory (can be overridden via --base_dir argument)
_DEFAULT_BASE_DIR = Path(r"C:\EWMRS") if platform.system() == "Windows" else Path.home() / "EWMRS"

# Get --base_dir from command line via IOManager
_arg_base_dir = IOManager.get_base_dir_arg()
if _arg_base_dir:
    BASE_DIR = Path(_arg_base_dir)
    io_manager.write_info(f"Using custom base directory: {BASE_DIR}")
else:
    BASE_DIR = _DEFAULT_BASE_DIR

def set_base_dir(path):
    """
    Dynamically update the base directory and all derived paths.
    
    Args:
        path (str or Path): New base directory path
    """
    global BASE_DIR, DATA_DIR, GUI_DIR
    global MRMS_RALA_DIR, MRMS_CGFLASH_DIR, MRMS_NLDN_DIR, MRMS_ECHOTOP18_DIR
    global MRMS_ECHOTOP30_DIR, MRMS_QPE_DIR, MRMS_RAIN_DIR, MRMS_PRECIPRATE_DIR
    global MRMS_PROBSEVERE_DIR, MRMS_FLASH_DIR, MRMS_VIL_DIR, MRMS_VII_DIR
    global MRMS_ROTATIONT_DIR, MRMS_COMPOSITE_DIR, MRMS_RHOHV_DIR, MRMS_PRECIPTYP_DIR
    global MRMS_MESH_DIR, MRMS_AZSHEARLOW_DIR, MRMS_AZSHEARMID_DIR, GOES_GLM_DIR, STORMCELL_JSON
    global GUI_RALA_DIR, GUI_NLDN_DIR, GUI_ECHOTOP18_DIR, GUI_ECHOTOP30_DIR
    global GUI_QPE_DIR, GUI_PRECIPRATE_DIR, GUI_PROBSEVERE_DIR, GUI_FLASH_DIR
    global GUI_VIL_DIR, GUI_VII_DIR, GUI_ROTATIONT_DIR, GUI_COMPOSITE_DIR
    global GUI_RHOHV_DIR, GUI_PRECIPTYP_DIR, GUI_MAP_DIR, GUI_MANIFEST_JSON
    
    BASE_DIR = Path(path)
    io_manager.write_info(f"Base directory updated to: {BASE_DIR}")
    
    # Reinitialize all paths
    _init_paths()

def _init_paths():
    """Initialize all path variables based on current BASE_DIR."""
    global DATA_DIR, GUI_DIR
    global MRMS_RALA_DIR, MRMS_CGFLASH_DIR, MRMS_NLDN_DIR, MRMS_ECHOTOP18_DIR
    global MRMS_ECHOTOP30_DIR, MRMS_QPE_DIR, MRMS_RAIN_DIR, MRMS_PRECIPRATE_DIR
    global MRMS_PROBSEVERE_DIR, MRMS_FLASH_DIR, MRMS_VIL_DIR, MRMS_VII_DIR
    global MRMS_ROTATIONT_DIR, MRMS_COMPOSITE_DIR, MRMS_RHOHV_DIR, MRMS_PRECIPTYP_DIR
    global MRMS_MESH_DIR, MRMS_AZSHEARLOW_DIR, MRMS_AZSHEARMID_DIR, GOES_GLM_DIR, STORMCELL_JSON
    global GUI_RALA_DIR, GUI_NLDN_DIR, GUI_ECHOTOP18_DIR, GUI_ECHOTOP30_DIR
    global GUI_QPE_DIR, GUI_PRECIPRATE_DIR, GUI_PROBSEVERE_DIR, GUI_FLASH_DIR
    global GUI_VIL_DIR, GUI_VII_DIR, GUI_ROTATIONT_DIR, GUI_COMPOSITE_DIR
    global GUI_RHOHV_DIR, GUI_PRECIPTYP_DIR, GUI_MAP_DIR, GUI_MANIFEST_JSON
    
    # ---------- PATH CONFIG ----------
    DATA_DIR = BASE_DIR / "data"
    MRMS_RALA_DIR = DATA_DIR / "RALA"
    MRMS_CGFLASH_DIR = DATA_DIR / "NLDN"
    MRMS_NLDN_DIR = DATA_DIR / "NLDN_Density"
    MRMS_ECHOTOP18_DIR = DATA_DIR / "EchoTop18"
    MRMS_ECHOTOP30_DIR = DATA_DIR / "EchoTop30"
    MRMS_QPE_DIR = DATA_DIR / "QPE_01H"
    MRMS_RAIN_DIR = DATA_DIR / "WarmRainProbability"
    MRMS_PRECIPRATE_DIR = DATA_DIR / "PrecipRate"
    MRMS_PROBSEVERE_DIR = DATA_DIR / "ProbSevere"
    MRMS_FLASH_DIR = DATA_DIR / "FLASH"
    MRMS_VIL_DIR = DATA_DIR / "VILDensity"
    MRMS_VII_DIR = DATA_DIR / "VII"
    MRMS_ROTATIONT_DIR = DATA_DIR / "RotationTrack30min"
    MRMS_COMPOSITE_DIR = DATA_DIR / "CompRefQC"
    MRMS_RHOHV_DIR = DATA_DIR / "RhoHV"
    MRMS_PRECIPTYP_DIR = DATA_DIR / "PrecipFlag"
    MRMS_MESH_DIR = DATA_DIR / "MESH"
    MRMS_AZSHEARLOW_DIR = DATA_DIR / "AzShearLow"
    MRMS_AZSHEARMID_DIR = DATA_DIR / "AzShearMid"
    GOES_GLM_DIR = DATA_DIR / "GLM"
    STORMCELL_JSON = DATA_DIR / "stormcells.json"
    
    # ---------- GUI PATH CONFIG ----------
    GUI_DIR = BASE_DIR / "gui"
    GUI_RALA_DIR = GUI_DIR / "RALA"
    GUI_NLDN_DIR = GUI_DIR / "NLDN"
    GUI_ECHOTOP18_DIR = GUI_DIR / "EchoTop18"
    GUI_ECHOTOP30_DIR = GUI_DIR / "EchoTop30"
    GUI_QPE_DIR = GUI_DIR / "QPE_01H"
    GUI_PRECIPRATE_DIR = GUI_DIR / "PrecipRate"
    GUI_PROBSEVERE_DIR = GUI_DIR / "ProbSevere"
    GUI_FLASH_DIR = GUI_DIR / "FLASH"
    GUI_VIL_DIR = GUI_DIR / "VILDensity"
    GUI_VII_DIR = GUI_DIR / "VII"
    GUI_ROTATIONT_DIR = GUI_DIR / "RotationTrack30min"
    GUI_COMPOSITE_DIR = GUI_DIR / "CompRefQC"
    GUI_RHOHV_DIR = GUI_DIR / "RhoHV"
    GUI_PRECIPTYP_DIR = GUI_DIR / "PrecipFlag"
    GUI_MAP_DIR = GUI_DIR / "maps"
    GUI_MANIFEST_JSON = GUI_DIR / "overlay_manifest.json"

# Initialize paths on module load
_init_paths()

# ---------- COLORMAP JSON LOOKUP ----------
def _find_colormap_json():
    """Locate colormaps.json in sensible locations."""
    candidates = [
        Path.cwd() / "colormaps.json",
        Path(__file__).resolve().parents[1] / "colormaps.json",  # EWMRS/colormaps.json
        Path(__file__).resolve().parents[2] / "colormaps.json",  # repo root/colormaps.json
        GUI_DIR / "colormaps.json",
    ]
    
    for candidate in candidates:
        if candidate.exists():
            io_manager.write_debug(f"Using colormap JSON: {candidate}")
            return candidate
    
    io_manager.write_warning("colormaps.json not found in common locations; using relative path 'colormaps.json'")
    return Path("colormaps.json")

GUI_COLORMAP_JSON = _find_colormap_json()


# ---------- FILE UTILITIES ----------
def latest_files(dir, n):
    """
    Return the n most recent files in a directory as a list (oldest to newest), excluding .idx files
    Inputs:
    - dir: Directory
    - n: Number of files
    Outputs:
    - List of files (oldest to newest) in the directory
    """
    if not dir.exists():
        io_manager.write_warning(f"{dir} doesn't exist!")
        return
    files = sorted(
        [f for f in dir.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"Not enough files in {dir}")
    return [str(f) for f in files[-n:]]

def clean_idx_files(folders):
    """
    Remove IDX files in a specified list of folders.
    Inputs:
    - folders: list of folders you want to remove IDX files from
    """
    for folder in folders:
        if folder.exists():
            idx_files = list(folder.rglob("*.idx"))
            if len(idx_files) == 0:
                io_manager.write_debug(f"No IDX files in folder: {folder}")
                return
            else:
                deleted_files = 0
                for f in idx_files:
                    try:
                        f.unlink()
                        deleted_files += 1
                    except Exception as e:
                        io_manager.write_error(f"Failed to delete IDX file {f}: {e}")
                
                if deleted_files > 0:
                    io_manager.write_debug(f"Deleted {deleted_files} files in {folder}")
        else:
            io_manager.write_error(f"Folder not found: {folder}")

# ---------- CLEANUP ----------
def clean_old_files(directory: Path, max_age_minutes=60):
    # Safety Check: Ensure directory is within BASE_DIR
    try:
        # resolve() handles symlinks and . and .. components
        # is_relative_to (Python 3.9+) checks if BASE_DIR is a parent of directory
        if not directory.resolve().is_relative_to(BASE_DIR.resolve()):
             io_manager.write_error(f"SAFETY ERROR: Attempting to clean {directory} which is not inside {BASE_DIR}")
             return
    except Exception as e:
        # Fallback/Safety catch
        io_manager.write_error(f"Safety check failed for path {directory}: {e}")
        return

    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    files_deleted = 0
    kept_files = []

    for f in directory.glob("*"):
        if f.is_file():
            try:
                mtime = f.stat().st_mtime
                if mtime < cutoff:
                    f.unlink()
                    files_deleted += 1
                else:
                    kept_files.append((f, mtime))
            except Exception as e:
                io_manager.write_error(f"Could not process/delete {f.name}: {e}")

    if files_deleted > 0:
        io_manager.write_debug(f"Deleted {files_deleted} files in {directory}")