
import unittest
import numpy as np
import xarray as xr
import pandas as pd

class TestAzShearDownsampling(unittest.TestCase):
    def setUp(self):
        # Create a synthetic 0.005 degree grid
        # Let's say we have a 4x4 grid (small scale test)
        # Latitudes: 20.0175, 20.0125, 20.0075, 20.0025 (descending like GRIB)
        # Longitudes: 230.0025, 230.0075, 230.0125, 230.0175 (ascending)
        
        self.lats = np.array([20.0175, 20.0125, 20.0075, 20.0025])
        self.lons = np.array([230.0025, 230.0075, 230.0125, 230.0175])
        
        # Create data with specific hotspots
        # We want to verify that the MAX value in a 2x2 block is preserved
        
        # Block 1 (Top-Left): Lats [20.0175, 20.0125], Lons [230.0025, 230.0075]
        # Target Output Lat: Mean(20.0175, 20.0125) = 20.015
        # Target Output Lon: Mean(230.0025, 230.0075) = 230.005
        
        data = np.zeros((4, 4))
        
        # Set a peak in Block 1
        data[0, 0] = 10.0 # Peak
        data[0, 1] = 5.0
        data[1, 0] = 2.0
        data[1, 1] = 3.0
        
        # Set a peak in Block 4 (Bottom-Right)
        data[2, 2] = 1.0
        data[2, 3] = 4.0
        data[3, 2] = 8.0 # Peak
        data[3, 3] = 6.0
        
        self.ds = xr.Dataset(
            data_vars=dict(
                unknown=(["latitude", "longitude"], data)
            ),
            coords=dict(
                latitude=self.lats,
                longitude=self.lons
            )
        )
        
        print("Setup synthetic dataset with shape:", self.ds['unknown'].shape)

    def test_coarsen_max_preserves_peaks(self):
        print("\nRunning coarsen().max() transformation...")
        
        # APPLY THE LOGIC
        # lat step is approx 0.005. coarsen=2 makes it 0.01
        # coord_func='mean' centers the new pixel
        downsampled_ds = self.ds.coarsen(latitude=2, longitude=2, boundary='trim', coord_func='mean').max()
        
        print("Output shape:", downsampled_ds['unknown'].shape)
        
        # Verify Shape
        self.assertEqual(downsampled_ds['unknown'].shape, (2, 2))
        
        # Verify Coordinates (Alignment with MRMS Standard Grid)
        # Expected Lats: 20.015, 20.005
        # Expected Lons: 230.005, 230.015
        
        expected_lats = np.array([20.015, 20.005])
        expected_lons = np.array([230.005, 230.015])
        
        np.testing.assert_allclose(downsampled_ds.latitude.values, expected_lats, atol=1e-5)
        np.testing.assert_allclose(downsampled_ds.longitude.values, expected_lons, atol=1e-5)
        print("Coordinates aligned correctly.")
        
        # Verify Values (Peak Preservation)
        # Block 1 Max should be 10.0
        self.assertEqual(downsampled_ds['unknown'].values[0, 0], 10.0)
        
        # Block 4 Max should be 8.0
        self.assertEqual(downsampled_ds['unknown'].values[1, 1], 8.0)
        
        print("Peaks preserved correctly.")
        print("Verification SUCCESS.")

if __name__ == '__main__':
    unittest.main()
