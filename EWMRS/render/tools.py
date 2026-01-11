import xarray as xr
import json
from datetime import datetime
from ..util.io import IOManager
from pathlib import Path
import re
import numpy as np
from pyproj import Transformer

io_manager = IOManager("[Transform]")

# Cached transformer for EPSG:4326 to EPSG:3857 (thread-safe per pyproj docs)
_TRANSFORMER_4326_TO_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

class TransformUtils:
    @staticmethod
    def load_ds(ds_path: Path, lat_limits=None, lon_limits=None):
        """
        Loads .grib2/.nc datasets
        Args:
         - ds_path: Pathlib Path() object of grib2 or netCDF dataset
         - lat_limits, lon_limits: 0-360 format of lat/lon limits (Only works on netCDF)
        
        Returns:
         - ds: Loaded dataset
        """
        
        io_manager.write_debug(f"Opening file: {ds_path} ...")

        try:
            if str(ds_path).endswith(".grib2") or str(ds_path).endswith(".grib"):
                if lat_limits or lon_limits:
                    io_manager.write_warning("lat/lon limits not supported with GRIB files, skipping ... ")
                
                ds = xr.open_dataset(ds_path, decode_timedelta=True)
                io_manager.write_debug(f"Successfully loaded dataset: {ds_path}")
                return ds
        
            if str(ds_path).endswith(".nc"):
                ds = xr.open_dataset(ds_path, decode_timedelta=True)

                if lat_limits and lon_limits:
                    # Latitude/Longitude variables: 'latitude', 'longitude'
                    ds = ds.sel(
                        latitude=slice(lat_limits[0], lat_limits[1]),
                        longitude=slice(lon_limits[0], lon_limits[1])
                    )
                    io_manager.write_debug(f"Loaded dataset subset with lat {lat_limits}, lon {lon_limits}")

                else:
                    io_manager.write_warning("lat/lon coordinates not specified, loading full dataset")

                io_manager.write_debug("Successfully loaded full dataset")
                return ds
        
        except Exception as e:
            io_manager.write_error(f"Failed to load dataset - {e}")
            return
    
    @staticmethod
    def find_timestamp(filepath):
        """
        Finds timestamps in a file based on predetermined patterns
        """
        filename = Path(filepath).name
        io_manager.write_debug(f"Extracting timestamp from filename: {filename}")
        
        patterns = [
            r'MRMS_MergedReflectivityQC_(\d{8})-(\d{6})',
            r'(\d{8})-(\d{6})_renamed',
            r'(\d{8}-\d{6})',
            r'.*(\d{8})-(\d{6}).*',
            r's(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)'
        ]
        
        for pattern_idx, pattern in enumerate(patterns):
            match = re.search(pattern, filename)
            if match:
                groups = match.groups()
                
                if len(groups) == 2:
                    date_str, time_str = groups
                elif len(groups) == 1 and len(groups[0]) >= 15:  # 'YYYYMMDD-HHMMSS' min length
                    combined = groups[0]
                    date_str, time_str = combined[:8], combined[9:15]
                else:
                    # fallback to next pattern
                    continue

                try:
                    formatted_time = (f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T"
                                    f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}")
                    io_manager.write_debug(f"Extracted timestamp: {formatted_time}")
                    return formatted_time
                except (IndexError, ValueError) as e:
                    io_manager.write_debug(f"Error formatting timestamp: {e}")
                    continue
        
        fallback = datetime.utcnow().isoformat()
        io_manager.write_debug(f"Using fallback timestamp: {fallback}")
        return fallback
    
    @staticmethod
    def get_domain_dimensions_km(ds):
        """
        Calculates the width and height of the domain in kilometers.
        
        Args:
            ds (xr.Dataset): Dataset with latitude/longitude coordinates
            
        Returns:
            (width_km, height_km): Dimensions of the domain in km
        """
        # Get lat/lon coordinate names
        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        
        # Get bounds
        lat_min = float(ds[lat_name].min())
        lat_max = float(ds[lat_name].max())
        lon_min = float(ds[lon_name].min())
        lon_max = float(ds[lon_name].max())
        
        # Approximate conversion (assuming spherical earth for simplicity or WGS84 approximation)
        # 1 degree latitude ~= 111.32 km
        height_km = (lat_max - lat_min) * 111.32
        
        # 1 degree longitude ~= 40075 km * cos(lat) / 360
        # Use average latitude for width calculation
        avg_lat = (lat_min + lat_max) / 2
        width_km = (lon_max - lon_min) * 40075 * np.cos(np.radians(avg_lat)) / 360
        
        return width_km, height_km

    @staticmethod
    def reproject_to_epsg3857(ds):
        """
        [DEPRECATED] Reproject an xarray Dataset from EPSG:4326 (WGS84) to EPSG:3857 (Web Mercator).
        This method is kept for backward compatibility but is no longer used in the main render pipeline.
        """
        io_manager.write_warning("TransformUtils.reproject_to_epsg3857 is deprecated and should not be used.")
        return ds


class OverlayManifestUtils:
    """
    Utility class to manage overlay manifest information for render layers.
    Stores details including name, colormap, latest image path, and fixed bounds.
    """

    def __init__(self):
        self.layers = []
        # Fixed bounds in EPSG:3857 (Web Mercator) - in meters
        # Original bounds: 20-55 N, 230-300 E (or -130 to -60 W)
        # Transformed to Web Mercator:
        # West: -130° → ~-14,465,442 m
        # East: -60° → ~-6,679,169 m
        # South: 20° → ~2,273,031 m
        # North: 55° → ~7,361,866 m
        self.bounds = {
            'north': 7361866,     # ~55°N in Web Mercator meters
            'south': 2273031,     # ~20°N in Web Mercator meters
            'west': -14465442,    # ~-130°W in Web Mercator meters
            'east': -6679169      # ~-60°W in Web Mercator meters
        }

    def validate_bounds(self, bounds):
        """
        Validates that the provided bounds dict matches the format of self.bounds.

        Args:
            bounds (dict): Bounds dict to validate

        Raises:
            ValueError: If bounds is not a dict, missing keys, or values are not numeric
        """
        if not isinstance(bounds, dict):
            raise ValueError("Bounds must be a dictionary")
        required_keys = set(self.bounds.keys())
        if set(bounds.keys()) != required_keys:
            raise ValueError(f"Bounds must have keys: {required_keys}")
        for key, value in bounds.items():
            if not isinstance(value, (int, float)):
                raise ValueError(f"Bounds value for '{key}' must be numeric")

    def add_layer(self, name: str, colormap: str, latest_image: str, timestamp: str, bounds=None):
        """
        Adds a new layer to the manifest.

        Args:
            name (str): Name of the layer
            colormap (str): Colormap key used for the layer
            latest_image (str): Path to the latest image file for the layer
            timestamp (str): Timestamp of the latest file
            bounds (dict, optional): Custom bounds dict. If None, uses self.bounds
        """
        try:
            if bounds is not None:
                self.validate_bounds(bounds)
        except Exception as e:
            io_manager.write_warning(f"Error in validating bounds - {e}; defaulting to default bounds")
        layer = {
            'name': name,
            'colormap': colormap,
            'latest_image': latest_image,
            'timestamp': timestamp,
            'bounds': self.bounds if bounds is None else bounds
        }
        self.layers.append(layer)

    def get_layers(self):
        """
        Returns the list of all stored layers.

        Returns:
            list: List of layer dictionaries
        """
        return self.layers

    def clear_layers(self):
        """
        Clears all stored layers.
        """
        self.layers = []

    def save_to_json(self, filepath: str):
        """
        Saves the layers to a JSON file.

        Args:
            filepath (str): Path to the JSON file to save
        """
        with open(filepath, 'w') as f:
            json.dump(self.layers, f, indent=4)
        io_manager.write_debug(f"Saved overlay manifest to {filepath}")