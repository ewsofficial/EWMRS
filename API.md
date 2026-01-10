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
