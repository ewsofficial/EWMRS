const express = require('express');
const fs = require('fs').promises;
const path = require('path');
const cors = require('cors');
const morgan = require('morgan');

const app = express();
app.use(cors());
app.use(morgan('tiny'));

// Determine BASE_DIR with parity to Python `util/file.py` behaviour but
// supporting multiple environments:
// - If `BASE_DIR` env var is set, use it.
// - On Windows: `C:\EdgeWARN_input`
// - On Linux/macOS inside a GitHub/workspaces container (cwd starts with `/workspaces`):
//   `/workspaces/EdgeWARN_input`
// - Otherwise on Linux/macOS: `/home/EdgeWARN_input`
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

// Known GUI subdirectories (keeps parity with util/file.py)
const GUI_SUBDIRS = [
  'RALA',
  'NLDN',
  'EchoTop18',
  'EchoTop30',
  'QPE_01H',
  'PrecipRate',
  'ProbSevere',
  'FLASH',
  'VILDensity',
  'VII',
  'RotationTrack30min',
  'CompRefQC',
  'RhoHV',
  'PrecipFlag',
  'maps'
];

async function listFilesInDir(dirPath, limit = 50) {
  try {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    const files = [];
    for (const ent of entries) {
      if (ent.isFile() && path.extname(ent.name).toLowerCase() !== '.idx') {
        const full = path.join(dirPath, ent.name);
        const stat = await fs.stat(full);
        files.push({ name: ent.name, mtime: stat.mtimeMs, size: stat.size });
      }
    }
    files.sort((a, b) => b.mtime - a.mtime); // newest first
    return files.slice(0, limit);
  } catch (err) {
    // If directory doesn't exist or can't be read, return null so caller can note it
    return null;
  }
}

// GET /renders - lists files in GUI dirs
// Optional query params:
// - limit: number of files per directory (default 50)
// - includeEmpty: if true, includes directories that exist but have no files
// Root endpoint to avoid default express 404 "Cannot GET /"
app.get('/', (req, res) => {
  res.json({
    service: 'EWMRS API',
    base_dir: BASE_DIR,
    gui_dir: GUI_DIR,
    endpoints: ['/renders', '/healthz']
  });
});
app.get('/renders', async (req, res) => {
  const limit = parseInt(req.query.limit || '50', 10);
  const includeEmpty = req.query.includeEmpty === 'true';

  const result = {};

  for (const sub of GUI_SUBDIRS) {
    const dpath = path.join(GUI_DIR, sub);
    const files = await listFilesInDir(dpath, limit);
    if (files && files.length > 0) {
      result[sub] = files;
    } else if (files && includeEmpty) {
      result[sub] = [];
    } else if (files === null) {
      // directory missing; include a small note
      result[sub] = { error: 'missing', path: dpath };
    }
  }

  res.json({ base_dir: BASE_DIR, gui_dir: GUI_DIR, renders: result });
});

// Serve an individual render file: /renders/:product/:file
app.get('/renders/:product/:file', async (req, res) => {
  const { product, file } = req.params;

  if (!GUI_SUBDIRS.includes(product)) {
    return res.status(404).json({ error: 'unknown product' });
  }

  const absGuiDir = path.resolve(GUI_DIR);
  const requested = path.resolve(absGuiDir, product, file);

  // Prevent path traversal: ensure requested path is inside the GUI directory
  if (!requested.startsWith(absGuiDir + path.sep) && requested !== absGuiDir) {
    return res.status(400).json({ error: 'invalid file path' });
  }

  try {
    await fs.access(requested);
  } catch (err) {
    return res.status(404).json({ error: 'file not found' });
  }

  // Use express to send the file; set root to absGuiDir to be explicit
  res.sendFile(requested, err => {
    if (err) {
      res.status(500).json({ error: 'failed to send file' });
    }
  });
});

// Simple healthcheck
app.get('/healthz', (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3003;
app.listen(PORT, () => {
  console.log(`EWMRS API server listening on port ${PORT}`);
  console.log(`Using BASE_DIR=${BASE_DIR}`);
});
