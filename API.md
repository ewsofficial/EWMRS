# EWMRS API Documentation

The EWMRS (Edge-compute Weather Map Rendering System) API provides access to rendered weather imagery and product information. It is built with Express.js and serves as the bridge between the backend rendering engine and the frontend displays.

## Overview

The API manages weather product renders stored in the `GUI_DIR`. It allows clients to:
1. Discover available weather products.
2. Fetch available historical timestamps for a specific product.
3. Download specific rendered PNG files.

## Base URL

By default, the API server runs on:
`http://localhost:3003`

The port can be configured via the `PORT` environment variable.

## Security & Rate Limiting

- **Rate Limiting**: The API is limited to **100 requests per 15 minutes** per IP address.
- **Security Headers**: Uses `helmet` for standard security headers and `cors` for cross-origin resource sharing.
- **Input Validation**: Endpoint parameters are sanitized to prevent directory traversal attacks.

---

## Endpoint Reference

### Root Information
`GET /`

Returns basic information about the service and available base endpoints.

**Response (JSON)**:
```json
{
  "service": "EWMRS API",
  "base_dir": "/home/EWMRS",
  "gui_dir": "/home/EWMRS/gui",
  "endpoints": ["/renders/get-items", "/renders/fetch", "/renders/download", "/healthz"]
}
```

### Health Check
`GET /healthz`

Simple endpoint to verify if the server is running.

**Response (JSON)**:
```json
{ "ok": true }
```

---

### Renders Module

All render-related endpoints are prefixed with `/renders`.

#### 1. List Available Products
`GET /renders/get-items`

Returns a list of all supported weather products that currently have data available on disk.

**Response (JSON)**:
```json
["CompRefQC", "EchoTop18", "RALA", "PrecipRate", ...]
```

#### 2. Fetch Product Timestamps
`GET /renders/fetch?product=[product_name]`

Returns a list of all available timestamps for a specific product, sorted from newest to oldest.

- **Parameters**:
    - `product`: The name of the product (e.g., `CompRefQC`).
- **Response (JSON)**:
    - Array of strings in `YYYYMMDD-HHMMSS` format.
    - `[]` if no data exists for the product.

#### 3. Download Render
`GET /renders/download?product=[product_name]&timestamp=[timestamp]`

Streams the PNG file for the requested product and timestamp.

- **Parameters**:
    - `product`: The name of the product.
    - `timestamp`: The timestamp in `YYYYMMDD-HHMMSS` format.
- **Response**:
    - `image/png` file stream if found.
    - `{"error": "File not found"}` with 404 status if not found.

#### 4. Get Tile
`GET /renders/tile?product=[product_name]&timestamp=[timestamp]&x=[x]&y=[y]`

Returns a single 250x250 PNG tile for the specified product, timestamp, and grid coordinates. The coordinate system uses (0,0) at the bottom-left corner.

- **Parameters**:
    - `product`: The name of the product (e.g., `CompRefQC`).
    - `timestamp`: The timestamp in `YYYYMMDD-HHMMSS` format.
    - `x`: Grid column index (0-based, left to right).
    - `y`: Grid row index (0-based, bottom to top).

- **Grid System**:
    - **Projection**: EPSG:3857 (Web Mercator)
    - **Tile Size**: 250x250 pixels
    - **Grid Dimensions**: 28 columns (x=0..27) x 14 rows (y=0..13)
    - **Total Tiles**: 392 per timestamp
    - **Geographic Extent (Meters)**:
        - West: `-14,471,533.8` (approx -130°W)
        - East: `-6,679,169.5` (approx -60°W)
        - South: `2,273,030.9` (approx 20°N)
        - North: `7,361,866.1` (approx 55°N)

- **Response**:
    - `image/png` file stream if found.
    - `{"error": "Tile not found"}` with 404 status.
    - `{"error": "Tile coordinates out of bounds"}` with 400 status.

#### 5. Get Tile Grid Info
`GET /renders/tile-info?product=[product_name]`

Returns the grid configuration and available timestamps for the specified product.

- **Parameters**:
    - `product`: The name of the product.

- **Response (JSON)**:
    ```json
    {
      "product": "CompRefQC",
      "rows": 14,
      "cols": 28,
      "tile_size": 250,
      "timestamps": ["20260212-120000", ...]
    }
    ```

#### 6. Get Colormaps
`GET /colormaps`

Returns the content of the `colormaps.json` configuration file, which defines the color scales and thresholds for various products.

- **Response (JSON)**:
    - The full JSON content of `colormaps.json`.
    - `{"error": "Failed to read colormaps.json"}` with 500 status if reading fails.

---

## Supported Products Reference

The API currently maps the following products to their respective MRMS file prefixes:

| Product Folder | MRMS File Prefix |
| :--- | :--- |
| `CompRefQC` | `MRMS_MergedReflectivityQC` |
| `EchoTop18` | `MRMS_EchoTop18` |
| `EchoTop30` | `MRMS_EchoTop30` |
| `RALA` | `MRMS_ReflectivityAtLowestAltitude` |
| `PrecipRate` | `MRMS_PrecipRate` |
| `VILDensity` | `MRMS_VILDensity` |
| `QPE_01H` | `MRMS_QPE` |

---

## Environment Configuration

The API respects the following environment variables:
- `BASE_DIR`: The root directory for EWMRS data. Defaults to `/home/EWMRS` on Linux or `C:\EWMRS` on Windows.
- `PORT`: The port to listen on (default: `3003`).
