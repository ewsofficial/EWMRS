/**
 * WPC Surface Analysis API Routes
 * 
 * Provides endpoints for accessing WPC surface analysis GeoJSON data.
 */

const express = require('express');
const fs = require('fs').promises;
const path = require('path');
const router = express.Router();

// Get BASE_DIR from app.locals (set in server.js)
function getWpcDir(req) {
    const baseDir = req.app.locals.BASE_DIR;
    return path.join(baseDir, 'wpc', 'surface_analysis');
}

/**
 * GET /wpc/surface-analysis
 * 
 * Returns the latest WPC surface analysis as GeoJSON.
 */
router.get('/surface-analysis', async (req, res) => {
    try {
        const wpcDir = getWpcDir(req);
        const latestPath = path.join(wpcDir, 'latest.geojson');

        const data = await fs.readFile(latestPath, 'utf-8');
        const geojson = JSON.parse(data);

        res.json(geojson);
    } catch (err) {
        if (err.code === 'ENOENT') {
            res.status(404).json({
                error: 'Surface analysis not found',
                message: 'No WPC surface analysis data available. The ingest process may not have run yet.'
            });
        } else {
            res.status(500).json({
                error: 'Failed to read surface analysis',
                details: err.message
            });
        }
    }
});

/**
 * GET /wpc/surface-analysis/timestamps
 * 
 * Returns a list of available surface analysis timestamps.
 */
router.get('/surface-analysis/timestamps', async (req, res) => {
    try {
        const wpcDir = getWpcDir(req);

        const entries = await fs.readdir(wpcDir, { withFileTypes: true });
        const timestamps = [];

        for (const entry of entries) {
            if (entry.isFile() && entry.name.endsWith('.geojson') && entry.name !== 'latest.geojson') {
                // Extract timestamp from filename: wpc_sfc_YYYYMMDD-HHz.geojson
                const match = entry.name.match(/wpc_sfc_(\d{8}-\d{2}z)\.geojson/);
                if (match) {
                    timestamps.push(match[1]);
                }
            }
        }

        // Sort descending (newest first)
        timestamps.sort((a, b) => b.localeCompare(a));

        res.json(timestamps);
    } catch (err) {
        if (err.code === 'ENOENT') {
            res.json([]);
        } else {
            res.status(500).json({
                error: 'Failed to list timestamps',
                details: err.message
            });
        }
    }
});

/**
 * GET /wpc/surface-analysis/:timestamp
 * 
 * Returns a specific surface analysis by timestamp.
 */
router.get('/surface-analysis/:timestamp', async (req, res) => {
    try {
        const wpcDir = getWpcDir(req);
        const timestamp = req.params.timestamp;

        // Validate timestamp format: YYYYMMDD-HHz
        if (!/^\d{8}-\d{2}z$/.test(timestamp)) {
            return res.status(400).json({
                error: 'Invalid timestamp format',
                message: 'Timestamp must be in YYYYMMDD-HHz format'
            });
        }

        const filePath = path.join(wpcDir, `wpc_sfc_${timestamp}.geojson`);
        const data = await fs.readFile(filePath, 'utf-8');
        const geojson = JSON.parse(data);

        res.json(geojson);
    } catch (err) {
        if (err.code === 'ENOENT') {
            res.status(404).json({
                error: 'Surface analysis not found',
                message: `No surface analysis found for timestamp: ${req.params.timestamp}`
            });
        } else {
            res.status(500).json({
                error: 'Failed to read surface analysis',
                details: err.message
            });
        }
    }
});

module.exports = router;
