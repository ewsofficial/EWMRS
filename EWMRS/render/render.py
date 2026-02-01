from pathlib import Path
import json
import numpy as np
from PIL import Image
from .tools import TransformUtils
from ..util import file as fs
from xarray import Dataset
from ..util.io import IOManager
from datetime import datetime
import threading

io_manager = IOManager("[Transform]")

# Colormap cache to avoid re-reading JSON on every render
_COLORMAP_CACHE = {}
_COLORMAP_CACHE_LOCK = threading.Lock()

class GUILayerRenderer:
    def __init__(self, dataset: Dataset, outdir: Path, colormap_key, file_name, timestamp):
        """
        Args:
            filepath (xr.Dataset): Dataset being converted to GUI png
            outdir (Path): Output directory of the converted png file
            colormap_key (str): Key of the color map as stored under colormaps.json
            file_name (str): Key of .png file name
            timestamp (str): ISO formatted timestamp string or string to parse
        """
        self.ds = dataset
        self.outdir = outdir
        self.colormap_key = colormap_key
        self.file_name = file_name
        self.timestamp = timestamp

    def _get_cmap(self):
        """
        Returns cached colormap data to avoid re-reading JSON file.
        
        Returns:
            thresholds (np.ndarray): array of dBZ or value thresholds
            colors (np.ndarray): array of RGB colors corresponding to thresholds
            interpolate (bool): whether to interpolate between colors
        """
        # Check cache first
        if self.colormap_key in _COLORMAP_CACHE:
            return _COLORMAP_CACHE[self.colormap_key]
        
        with _COLORMAP_CACHE_LOCK:
            # Double-check after acquiring lock
            if self.colormap_key in _COLORMAP_CACHE:
                return _COLORMAP_CACHE[self.colormap_key]
            
            with open(fs.GUI_COLORMAP_JSON, 'r') as f:
                cmaps_json = json.load(f)

            # Iterate through all colormaps to find the matching key
            for source in cmaps_json:
                for cmap in source.get("colormaps", []):
                    if cmap.get("name") == self.colormap_key:
                        thresholds = np.array([t["value"] for t in cmap["thresholds"]])
                        colors = np.array([t["rgb"] for t in cmap["thresholds"]], dtype=np.float32)
                        interpolate = cmap.get("interpolate", True)
                        result = (thresholds, colors, interpolate)
                        _COLORMAP_CACHE[self.colormap_key] = result
                        return result
            
            # If key not found, raise an error with the path we tried
            raise ValueError(f"Colormap '{self.colormap_key}' not found in {fs.GUI_COLORMAP_JSON}")

    def convert_to_png(self):
        """
        Converts dataset to a png file and then saves it to outdir.
        Reprojects data to EPSG:3857 (Web Mercator) projection.
        """

        # Step 1: No Reprojection needed for 1km/pixel raw render
        # We will resize the output image based on physical domain size later
        data = self.ds['unknown'].values

        # Step 2: Get colormap
        thresholds, colors, interpolate = self._get_cmap()

        # Step 2.5: Apply colormap
        # Use ravel() to avoid copy if possible, though digitize/interp might flatten anyway
        flat_data = data.ravel()

        # Pre-allocate output array (N, 4) in uint8 to save memory
        N = flat_data.shape[0]
        rgba_flat = np.empty((N, 4), dtype=np.uint8)

        if interpolate:
            # Interpolate directly into the output array channels
            # Casting to uint8 immediately saves memory compared to keeping full float arrays
            rgba_flat[:, 0] = np.interp(flat_data, thresholds, colors[:, 0]).astype(np.uint8)
            rgba_flat[:, 1] = np.interp(flat_data, thresholds, colors[:, 1]).astype(np.uint8)
            rgba_flat[:, 2] = np.interp(flat_data, thresholds, colors[:, 2]).astype(np.uint8)
        else:
            # Discrete color mapping
            indices = np.digitize(flat_data, thresholds) - 1
            indices = np.clip(indices, 0, len(colors) - 1)
            
            # Cast colors table to uint8 once
            colors_uint8 = colors.astype(np.uint8)

            # Map directly into the output array
            rgba_flat[:, :3] = colors_uint8[indices]

        # Alpha channel: transparent for values < first threshold
        # This ensures that values below the defined range (like AzShear 0 when min is 1) are transparent
        rgba_flat[:, 3] = np.where(flat_data < thresholds[0], 0, 255).astype(np.uint8)

        # Reshape to original grid
        # Note: Grib data is often (lat, lon), where lat is row (y), lon is col (x)
        # We want image to be (height, width) which corresponds to (lat, lon) shape
        rgba = rgba_flat.reshape((data.shape[0], data.shape[1], 4))

        # Step 3: Generate and save
        # Find timestamp
        try:
            dt = datetime.fromisoformat(self.timestamp)
        except ValueError:
            # Fallback if timestamp is a filename or path?
            # Assuming callers pass a valid ISO timestamp or we use TransformUtils if it looks like a path
            cleaned_ts = TransformUtils.find_timestamp(self.timestamp)
            dt = datetime.fromisoformat(cleaned_ts)
        
        # Force seconds to 00 for consistency and fast lookup
        timestamp = dt.strftime(r"%Y%m%d-%H%M00")

        # Ensure the output directory exists
        self.outdir.mkdir(parents=True, exist_ok=True)

        # Define the full file path
        png_file = self.outdir / f"{self.file_name}_{timestamp}.png"

        # Create the image and save
        img = Image.fromarray(rgba, mode="RGBA")
        
        img.save(png_file, compress_level=1)  # Fast compression (1=fastest, 9=smallest)

        io_manager.write_debug(f"Saved {self.file_name} PNG file to {png_file}")

        # Update index.json
        self._update_index(timestamp)

        return png_file, timestamp

    def _update_index(self, new_timestamp):
        """
        Updates the index.json file in the output directory with the new timestamp.
        Maintains a sorted, unique list of timestamps.
        """
        index_file = self.outdir / "index.json"
        timestamps = []

        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    timestamps = json.load(f)
            except Exception as e:
                io_manager.write_warning(f"Failed to read index.json in {self.outdir}: {e}. Creating new one.")

        if new_timestamp not in timestamps:
            timestamps.append(new_timestamp)
            timestamps.sort(reverse=True) # Newest first

            try:
                with open(index_file, 'w') as f:
                    json.dump(timestamps, f)
            except Exception as e:
                io_manager.write_error(f"Failed to update index.json in {self.outdir}: {e}")