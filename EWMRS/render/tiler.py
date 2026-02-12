"""Tile splitting module for render outputs.

This module provides functionality to split rendered images into tiles
with the coordinate origin (0,0) at the bottom-left corner.
"""

from typing import List, Tuple
import numpy as np
from PIL import Image


class TileSplitter:
    """Splits an image array into tiles with Y=0 at the bottom.
    
    The coordinate system uses:
    - X (column): 0 = left, increases rightward
    - Y (row): 0 = bottom, increases upward
    
    This requires a Y-flip transformation because image arrays in NumPy
    have row 0 at the top, but we want tile Y=0 to represent the bottom
    of the image.
    
    Attributes:
        image: The image array of shape (height, width, channels).
        tile_size: Size of each tile in pixels (default 250).
        grid_rows: Number of tile rows.
        grid_cols: Number of tile columns.
    """
    
    def __init__(self, image_array: np.ndarray, tile_size: int = 250):
        """Initialize the TileSplitter.
        
        Args:
            image_array: Image as numpy array of shape (height, width, channels).
            tile_size: Size of each tile in pixels. Default is 250.
        
        Raises:
            ValueError: If image dimensions are not exact multiples of tile_size.
        """
        if image_array.ndim != 3:
            raise ValueError(f"Expected 3D array (H, W, C), got shape {image_array.shape}")
        
        self.image = image_array
        self.tile_size = tile_size
        self.height, self.width = image_array.shape[:2]
        self.grid_rows = self.height // tile_size
        self.grid_cols = self.width // tile_size
        
        # Validate dimensions
        if self.height % tile_size != 0 or self.width % tile_size != 0:
            raise ValueError(
                f"Image dimensions ({self.height}x{self.width}) must be exact "
                f"multiples of tile_size ({tile_size})"
            )
    
    def get_tile(self, tile_x: int, tile_y: int) -> np.ndarray:
        """Extract a single tile at grid position (tile_x, tile_y).
        
        Args:
            tile_x: Column index (0 = left, increases rightward).
            tile_y: Row index (0 = bottom, increases upward).
        
        Returns:
            Tile as numpy array of shape (tile_size, tile_size, channels).
        
        Raises:
            IndexError: If tile coordinates are out of bounds.
        """
        if tile_x < 0 or tile_x >= self.grid_cols:
            raise IndexError(f"tile_x={tile_x} out of bounds [0, {self.grid_cols - 1}]")
        if tile_y < 0 or tile_y >= self.grid_rows:
            raise IndexError(f"tile_y={tile_y} out of bounds [0, {self.grid_rows - 1}]")
        
        # Y-flip: tile_y=0 should map to bottom of image
        # Array row 0 is at the top, so we need to invert the Y coordinate
        array_row_start = (self.grid_rows - 1 - tile_y) * self.tile_size
        array_row_end = array_row_start + self.tile_size
        
        # X is straightforward: left to right
        array_col_start = tile_x * self.tile_size
        array_col_end = array_col_start + self.tile_size
        
        return self.image[array_row_start:array_row_end,
                         array_col_start:array_col_end,
                         :]
    
    def split(self) -> List[Tuple[int, int, np.ndarray]]:
        """Split the entire image into tiles.
        
        Returns:
            List of tuples (x, y, tile_data) for all tiles.
            Tiles are returned in order from bottom-left to top-right
            (Y=0 first, then increasing Y; within each row, X increases).
        """
        tiles = []
        for tile_y in range(self.grid_rows):
            for tile_x in range(self.grid_cols):
                tile_data = self.get_tile(tile_x, tile_y)
                tiles.append((tile_x, tile_y, tile_data))
        return tiles
    
    def get_grid_info(self) -> dict:
        """Get grid configuration information.
        
        Returns:
            Dictionary with rows, cols, and tile_size.
        """
        return {
            "rows": self.grid_rows,
            "cols": self.grid_cols,
            "tile_size": self.tile_size
        }


def save_tile(tile_data: np.ndarray, output_path: str) -> None:
    """Save a tile as a PNG file.
    
    Args:
        tile_data: Tile as numpy array of shape (tile_size, tile_size, channels).
        output_path: Path to save the PNG file.
    """
    img = Image.fromarray(tile_data, mode="RGBA")
    img.save(output_path, compress_level=1)  # Fast compression
