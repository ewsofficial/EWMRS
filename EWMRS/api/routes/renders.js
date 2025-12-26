const express = require('express');
const router = express.Router();
const path = require('path');
const fs = require('fs').promises;

// Mapping: User/Folder Product Name -> File Prefix
// Derived from EWMRS/render/config.py
const PRODUCT_MAPPING = {
  'CompRefQC': 'MRMS_MergedReflectivityQC',
  'EchoTop18': 'MRMS_EchoTop18',
  'EchoTop30': 'MRMS_EchoTop30',
  'RALA': 'MRMS_ReflectivityAtLowestAltitude',
  'PrecipRate': 'MRMS_PrecipRate',
  'VILDensity': 'MRMS_VILDensity',
  'QPE_01H': 'MRMS_QPE',
  // Add other products as needed, these are the ones explicitly in config.py
  // For others in GUI_SUBDIRS (like NLDN, FLASH), assuming prefix might match or need adding.
  // Using a fallback strategy? The user wants "Keep the renders/download file lookup fast"
  // so we should probably rely on this mapping.
  // Let's add the rest based on convention if possible, or leave them for now
  // until explicitly added in python config.
  // For safety, we can try to guess or just support the ones we know.
};

// Base GUI Directory (injected or calculated essentially the same as server.js)
// We need to access BASE_DIR. Ideally it's passed or available.
// For now, let's grab it from process.env or re-calculate.
// To keep it DRY, simple re-calc here or middleware injection in server.js?
// Sticking to re-calc for independence.
const envBase = process.env.BASE_DIR;
let BASE_DIR;
if (envBase) {
  BASE_DIR = envBase;
} else if (process.platform === 'win32') {
  BASE_DIR = 'C:\\EdgeWARN_input';
} else {
  const cwd = process.cwd() || '';
  if (cwd.startsWith('/workspaces')) {
    BASE_DIR = '/workspaces/EdgeWARN_input';
  } else {
    BASE_DIR = '/home/EdgeWARN_input';
  }
}
const GUI_DIR = path.join(BASE_DIR, 'gui');

// GET /fetch/resources?product=PRODUCT
router.get('/fetch/resources', async (req, res) => {
  const product = req.query.product;

  if (!product) {
    return res.status(400).json({ error: 'Missing product parameter' });
  }

  // Security: Prevent directory traversal
  if (product.includes('..') || product.includes('/') || product.includes('\\')) {
    return res.status(400).json({ error: 'Invalid product name' });
  }

  const productDir = path.join(GUI_DIR, product);
  const indexFile = path.join(productDir, 'index.json');

  try {
    const data = await fs.readFile(indexFile, 'utf8');
    const timestamps = JSON.parse(data);
    
    // According to req "rounded down to the minute".
    // Our python script saves YYYYMMDD-HHMM00. 
    // This is effectively rounded down to the minute (seconds=00).
    // We serve this list directly.
    res.json(timestamps);
  } catch (err) {
    if (err.code === 'ENOENT') {
      // Index file doesn't exist yet, return empty list or 404? 
      // Empty list is friendlier for "no resources yet".
      return res.json([]);
    }
    console.error(`Error reading index.json for ${product}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /download/resources?product=PRODUCT&timestamp=TIMESTAMP
router.get('/download/resources', async (req, res) => {
  const { product, timestamp } = req.query;

  if (!product || !timestamp) {
    return res.status(400).json({ error: 'Missing product or timestamp parameter' });
  }

  // Security checks
  if (product.includes('..') || timestamp.includes('..')) {
    return res.status(400).json({ error: 'Invalid parameters' });
  }

  const filePrefix = PRODUCT_MAPPING[product];
  if (!filePrefix) {
    // If not in mapping, maybe try using product as prefix? 
    // Or return error. Safe default is error or strict mapping.
    // Given the list in config.py covers the main ones, we'll error if unknown
    // to avoid guessing wrong file names.
    return res.status(404).json({ error: 'Unknown product or no mapping found' });
  }

  // Construct filename: "{product_prefix}_{timestamp}.png"
  // e.g. MRMS_MergedReflectivityQC_20251226-123000.png
  const filename = `${filePrefix}_${timestamp}.png`;
  const filePath = path.join(GUI_DIR, product, filename);

  try {
    await fs.access(filePath);
    res.sendFile(filePath);
  } catch (err) {
    res.status(404).json({ error: 'File not found' });
  }
});

module.exports = router;
