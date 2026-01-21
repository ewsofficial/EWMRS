from EWMRS.ingest.mrms.config import get_mrms_modifiers, get_goes_modifiers, bucket
from EWMRS.ingest.mrms.s3_sync import FileFinder, FileDownloader
from EWMRS.ingest.mrms.s3_async import AsyncFileFinder, AsyncFileDownloader
from EWMRS.ingest.mrms.parse import parse_goes_bucket_path
from EWMRS.ingest.mrms.downloader import (
    download_all_files_async_internal,
    download_all_files_sync_fallback,
    download_all_goes_files,
    download_all_goes_files_async
)
from EWMRS.util.io import IOManager
import EWMRS.util.file as fs
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aioboto3
from botocore import UNSIGNED
from botocore.client import Config
import traceback

io_manager = IOManager("[Ingest]")

def download_all_files(dt, max_entries=10, remove_old_files=True):
    """
    Main function for downloading all MRMS files.

    This function is called by src/run.py without any modifications needed.
    It uses async operations internally for better performance while maintaining
    the same synchronous interface.
    """
    # Clear files first
    mrms_modifiers = get_mrms_modifiers()
    goes_modifiers_list = get_goes_modifiers()
    
    folders = [outdir for _, _, outdir in mrms_modifiers]
    # Add GOES folders
    folders.extend([outdir for _, outdir in goes_modifiers_list])
    if remove_old_files:
        for f in folders:
            fs.clean_old_files(f, max_age_minutes=60)

    # Use different function for stormcell dirs

    # Use async operations internally for better performance
    # This maintains the same API but with improved performance
    async def _download_all():
        await asyncio.gather(
            download_all_files_async_internal(dt, max_entries),
            download_all_goes_files_async(dt, max_entries)
        )

    try:
        asyncio.run(_download_all())
    except Exception as e:
        io_manager.write_error(f"Async downloads failed: {e}")
        io_manager.write_info("Falling back to synchronous downloads...")
        download_all_files_sync_fallback(dt, max_entries)
        # Fallback for GOES as well (synchronous)
        download_all_goes_files(dt, max_entries)