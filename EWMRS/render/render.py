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

        # Step 1: Reproject to EPSG:3857 (Web Mercator)
        self.ds = TransformUtils.reproject_to_epsg3857(self.ds)
        data = self.ds['unknown'].values

        # Step 2: Get colormap
        thresholds, colors, interpolate = self._get_cmap()

        # Step 2.5: Apply colormap
        flat_data = data.flatten()

        if interpolate:
            r = np.interp(flat_data, thresholds, colors[:, 0])
            g = np.interp(flat_data, thresholds, colors[:, 1])
            b = np.interp(flat_data, thresholds, colors[:, 2])
        else:
            # Discrete color mapping
            indices = np.digitize(flat_data, thresholds) - 1
            indices = np.clip(indices, 0, len(colors) - 1)
            
            # Use gathered indices to fetch colors
            # indices is an array of shape (N,)
            # colors is (M, 3)
            # colors[indices] is (N, 3)
            mapped_colors = colors[indices]
            r = mapped_colors[:, 0]
            g = mapped_colors[:, 1]
            b = mapped_colors[:, 2]
        a = np.where(flat_data < 0, 0, 255)  # transparent for values < 0

        # Reshape to original grid for 1:1 pixel correspondence
        rgba = np.stack([r, g, b, a], axis=1).reshape((data.shape[0], data.shape[1], 4)).astype(np.uint8)

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