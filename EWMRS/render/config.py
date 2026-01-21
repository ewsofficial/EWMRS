from ..util import file as fs

def get_file_list():
    """
    Get the render file configuration list.
    
    Returns list at call time to respect dynamic BASE_DIR changes.
    """
    return [
        {
            "name": "MRMS_MergedReflectivityQC",
            "colormap_key": "NWS_Reflectivity",
            "filepath": fs.MRMS_COMPOSITE_DIR,
            "outdir": fs.GUI_COMPOSITE_DIR
        },
        {
            "name": "MRMS_EchoTop18",
            "colormap_key": "EnhancedEchoTop",
            "filepath": fs.MRMS_ECHOTOP18_DIR,
            "outdir": fs.GUI_ECHOTOP18_DIR
        },
        {
            "name": "MRMS_EchoTop30",
            "colormap_key": "EnhancedEchoTop",
            "filepath": fs.MRMS_ECHOTOP30_DIR,
            "outdir": fs.GUI_ECHOTOP30_DIR
        },
        {
            "name": "MRMS_ReflectivityAtLowestAltitude",
            "colormap_key": "NWS_Reflectivity",
            "filepath": fs.MRMS_RALA_DIR,
            "outdir": fs.GUI_RALA_DIR
        },
        {
            "name": "MRMS_PrecipRate",
            "colormap_key": "PrecipRate",
            "filepath": fs.MRMS_PRECIPRATE_DIR,
            "outdir": fs.GUI_PRECIPRATE_DIR
        },
        {
            "name": "MRMS_VILDensity",
            "colormap_key": "VILDensity",
            "filepath": fs.MRMS_VIL_DIR,
            "outdir": fs.GUI_VIL_DIR
        },
        {
            "name": "MRMS_QPE",
            "colormap_key": "QPE_01H",
            "filepath": fs.MRMS_QPE_DIR,
            "outdir": fs.GUI_QPE_DIR
        },
        {
            "name": "MRMS_VII",
            "colormap_key": "VILDensity",
            "filepath": fs.MRMS_VII_DIR,
            "outdir": fs.GUI_VII_DIR
        }
    ]

# For backward compatibility - returns list at import time (use get_file_list() for dynamic paths)
file_list = get_file_list()