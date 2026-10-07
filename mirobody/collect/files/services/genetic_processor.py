import asyncio
import hashlib
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from collections.abc import Generator

from mirobody.utils.i18n import localize
from mirobody.kernel.ops import is_driver_exception
from mirobody.collect.files.services.file_db_service import FileDbService
from mirobody.collect.files.services import genotype_format
from mirobody.collect.files.services.genetic_store import GenotypeStore
from mirobody.translate.genotype import NORMALIZER_VERSION, infer_sex, normalize
from mirobody.translate.genotype_sites import SiteCatalog

logger = logging.getLogger(__name__)


#: A build is named only when this many calls sit on a known site and almost
#: all of them on one build's coordinates. The packaged examples hold 13 sites,
#: so no test ever reached a detected build until `build_from_votes` had one.
BUILD_MIN_VOTES = 20
BUILD_MIN_SHARE = 0.9


def build_from_votes(build_votes: dict[str, int]) -> str:
    """The genome build the calls' positions point to, or "unknown"."""
    total = sum(build_votes.values())
    if total < BUILD_MIN_VOTES:
        return "unknown"
    detected = max(build_votes, key=build_votes.get)
    return detected if build_votes[detected] / total >= BUILD_MIN_SHARE else "unknown"


class NotAGenotypeExport(ValueError):
    """The file is not one the genotype reader accepts. The message is shown to
    the person who uploaded it, so it names the fix and never the contents."""


class GeneticDataLoader:
    """Genetic data loader, supports large file streaming processing and progress updates"""

    def __init__(
        self,
        message_id=None,
        language="en",
        user_id=None,
        display_filename: str = None,
        display_file_size: int = None,
        file_key: str = None,  # New: file_key for th_files updates
        store: GenotypeStore | None = None,
    ):
        self.message_id = message_id
        self.language = language
        self.user_id = user_id
        self.display_filename = display_filename
        self.display_file_size = display_file_size
        self.file_key = file_key
        self.store = store or GenotypeStore()

    def parse_genetic_file(
        self,
        file_path: str,
        fmt: genotype_format.GenotypeFormat,
        *,
        sample: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Parse a raw genotype export, yielding one row per marker.

        The format comes from the file's own column header (`genotype_format`),
        so WeGene, 23andMe, AncestryDNA and MyHeritage exports all parse. This
        used to wait for the WeGene/23andMe comment header and take the first
        four columns: an AncestryDNA file never started, and had it started,
        would have kept allele1 and dropped allele2. A file whose header this
        does not recognise raises rather than yielding nothing, because "0
        rows saved" reads as success.
        """
        valid_records = 0
        with genotype_format.open_lines(file_path) as lines:
            for call in genotype_format.records(lines, fmt, sample=sample):
                valid_records += 1
                # An unannotated VCF uses "." in ID. Keep a stable local
                # identity until the public site table supplies an rsID.
                rsid = call.rsid
                if rsid == ".":
                    locus = f"{call.chromosome}:{call.position}:{call.ref}:{','.join(call.alt)}"
                    rsid = "loc:" + hashlib.sha256(locus.encode()).hexdigest()[:32]
                no_call = call.genotype_raw == genotype_format.NO_CALL or (
                    call.gt is not None and "." in call.gt.split("/") + call.gt.split("|")
                )
                yield {
                    "rsid": rsid,
                    "rsid_raw": call.rsid,
                    "chromosome": call.chromosome,
                    "position": call.position,
                    "genotype": call.genotype_raw,
                    "genotype_raw": call.genotype_raw,
                    "strand": call.strand,
                    "ref": call.ref,
                    "alt": ",".join(call.alt) if call.alt else None,
                    "gt": call.gt,
                    "call_status": "no_call" if no_call else "called" if call.gt and call.strand == "plus" else "unresolved",
                    "strand_check": "vcf_plus" if call.gt else "top_unresolved" if call.strand == "top" else None,
                }
        logger.info("genotype file parsed: row_count=%d", valid_records)

    async def update_progress(self, processed: int, saved: int, message: str, *, done: bool = False) -> None:
        """Record a load's progress on its `th_files` row and tell the uploader's
        socket: half way (50) while rows are parsed, 100 when `done`."""
        if not self.file_key:
            return
        from mirobody.collect.files.file_upload_manager import get_websocket_file_upload_manager

        progress_percent = 100 if done else 50
        content = localize("genetic_progress_display", self.language, "load_genetic_data",
                           processed=processed, saved=saved, percent=progress_percent)
        await FileDbService.update_file_content(
            file_key=self.file_key,
            updates={
                "status": "processing",
                "progress": progress_percent,
                "message": content,
                "timestamp": datetime.now(UTC).isoformat(),
                "progress_details": {
                    "processed": processed,
                    "saved": saved,
                    "progress_percent": progress_percent,
                    "stage": "genetic_processing"
                },
                "raw": content,
            }
        )
        if not self.message_id:
            return
        # A closed socket costs the uploader the live count, never the load.
        try:
            await get_websocket_file_upload_manager().send_message_by_message_id(self.message_id, {
                "type": "upload_progress", "messageId": self.message_id,
                "status": "processing", "progress": progress_percent,
                "message": message, "file_type": "genetic",
                "filename": self.display_filename, "success": False,
                "file_key": self.file_key,
                "processing_stats": {"processed_records": processed, "saved_records": saved,
                                     "progress_percent": progress_percent},
            })
        except Exception as e:
            logger.warning("genotype progress not sent: message_id=%s error_type=%s", self.message_id,
                           type(e).__name__, exc_info=not is_driver_exception(e))

    async def load_user_genetic_data(
        self,
        user_id: str,
        file_path: str,
        batch_size: int = 50000,
        is_up_progress: bool = True,
        source_table_id: str | None = None,
        sample: str | None = None,
    ) -> int:
        """Load a new set, publishing it only after every parsed row is stored.

        Parsing a row, looking it up in the site catalogue and normalising it
        is CPU work, 1.2 s per 50,000 rows (15 s for a 23andMe export): it
        runs a batch at a time on one worker thread (`_Batches`), so the event
        loop answers other requests while a file loads. One thread, because
        the catalogue is a sqlite connection, which serves only the thread
        that opened it."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File does not exist: {file_path}")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        with open(file_path, "rb") as probe:
            fmt = genotype_format.sniff_stream(probe)
        if fmt is None:
            raise NotAGenotypeExport("No rsid / chromosome / position / genotype column header was found.")
        if fmt.shape == genotype_format.SHAPE_VCF and len(fmt.samples) != 1 and sample is None:
            raise NotAGenotypeExport("This VCF holds more than one sample. Upload a single-sample VCF.")

        if is_up_progress:
            await self.update_progress(0, 0, localize("genetic_parsing_start", self.language, "load_genetic_data"))
        loop = asyncio.get_running_loop()
        total_saved = batch_count = 0
        set_id: int | None = None
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="genotype") as worker:
            batches = await loop.run_in_executor(worker, _Batches, self, file_path, fmt, sample, batch_size)
            try:
                format_id = getattr(fmt, "format_id", "") or f"{(fmt.vendor or 'generic').lower()}_{fmt.shape}"
                set_id = await self.store.create_set(
                    user_id,
                    file_key=source_table_id or self.file_key,
                    format_id=format_id[:40],
                    vendor=fmt.vendor,
                    build_declared=fmt.build,
                    normalizer_version=NORMALIZER_VERSION,
                    site_table_version=batches.catalog_version,
                )
                while batch := await loop.run_in_executor(worker, batches.next):
                    if is_up_progress and len(batch) == batch_size:
                        await self.update_progress(
                            batches.total, total_saved,
                            localize("genetic_parsing_progress", self.language, "load_genetic_data",
                                     total=batches.total))
                    await self.store.write_batch(set_id, batch)
                    total_saved += len(batch)
                    batch_count += 1
                    if is_up_progress and batch_count % 5 == 0:
                        await self.update_progress(
                            batches.total, total_saved,
                            localize("genetic_batch_status", self.language, "load_genetic_data", batches=batch_count))

                sex = infer_sex(x_total=batches.x_total, x_heterozygous=batches.x_heterozygous,
                                y_called=batches.y_called)
                await self.store.activate_set(
                    set_id, user_id, n_rows=batches.total, n_called=batches.n_called,
                    build_detected=build_from_votes(batches.build_votes), sex_inferred=sex,
                )
                if is_up_progress:
                    await self.update_progress(
                        batches.total, total_saved,
                        localize("genetic_processing_finished", self.language, "load_genetic_data",
                                 total=batches.total, saved=total_saved),
                        done=True,
                    )
                logger.info("genotype set activated", extra={"user_id": user_id, "row_count": total_saved, "set_id": set_id})
                return total_saved

            except Exception as e:
                if set_id is not None:
                    try:
                        await self.store.fail_set(set_id)
                    except Exception as fail_error:
                        logger.error(
                            "genotype set failure marker failed: error_type=%s",
                            type(fail_error).__name__,
                            exc_info=not is_driver_exception(fail_error),
                        )
                if is_up_progress:
                    await self.update_progress(batches.total, total_saved, "Genotype processing failed")
                logger.error("genotype loading failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
                raise
            finally:
                await loop.run_in_executor(worker, batches.close)


class _Batches:
    """A genotype file read into normalised rows a batch at a time, with the
    counts the activation needs. Built, read and closed on one worker thread:
    the site catalogue is a sqlite connection bound to its thread."""

    def __init__(self, loader: GeneticDataLoader, file_path: str, fmt: genotype_format.GenotypeFormat,
                 sample: str | None, size: int) -> None:
        self._rows = loader.parse_genetic_file(file_path, fmt, sample=sample)
        self._catalog = SiteCatalog()
        self._size = size
        self.catalog_version = self._catalog.version
        self.total = self.n_called = 0
        self.x_total = self.x_heterozygous = self.y_called = 0
        self.build_votes = {"GRCh37": 0, "GRCh38": 0}

    def next(self) -> list[dict[str, Any]]:
        """The next batch, [] when the file is read."""
        batch: list[dict[str, Any]] = []
        for record in self._rows:
            site = self._catalog.lookup(record["rsid_raw"], record["chromosome"], record["position"])
            normalized = normalize(
                rsid=record["rsid"], chrom=record["chromosome"], position=record["position"],
                genotype=record["genotype"], strand=record["strand"],
                vcf_gt=record["gt"], vcf_ref=record["ref"], vcf_alt=record["alt"],
                site=site,
            )
            record.update({
                "rsid": normalized.rsid, "chromosome": normalized.chrom,
                "pos37": normalized.pos37, "pos38": normalized.pos38,
                "ref": normalized.ref, "alt": normalized.alt, "gene": normalized.gene,
                "gt": normalized.gt, "call_status": normalized.call_status,
                "zygosity": normalized.zygosity, "strand_check": normalized.strand_check,
            })
            if normalized.matched_build:
                self.build_votes[normalized.matched_build] += 1
            if record["chromosome"] == "X" and record["genotype"] != genotype_format.NO_CALL:
                self.x_total += 1
                alleles = record["genotype"].replace("/", "").replace("|", "")
                self.x_heterozygous += len(alleles) == 2 and alleles[0] != alleles[1]
            if record["chromosome"] == "Y" and record["genotype"] != genotype_format.NO_CALL:
                self.y_called += 1
            self.total += 1
            self.n_called += record["call_status"] == "called"
            batch.append(record)
            if len(batch) == self._size:
                break
        return batch

    def close(self) -> None:
        self._rows.close()
        self._catalog.close()


async def process_genetic_file(
    user_id: str,
    temp_file_path: Path,
    message_id: str = None,
    language: str = "en",
    original_filename: str = None,  # New: original filename parameter
    original_file_size: int = None,  # New: original file size parameter
    source_table: str = None,  # New: data source table name
    source_table_id: str = None,  # New: data source table record ID
    file_key: str = None,  # New: file_key for th_files updates
    full_url: str = None,  # New: OSS/S3 URL for the file
    file_abstract: str = None,  # New: file abstract/summary
    target_user_id: str = None,  # Data owner ID for the genotype set
):
    """Entry function for processing genetic data files - writes to th_files table
    
    Args:
        user_id: Uploader ID (for WebSocket notifications)
        target_user_id: Data owner ID (for th_series_data_genetic.user_id)
        ... other params
    """
    temp_file_path = Path(temp_file_path)
    try:
        # Import websocket manager locally to avoid circular import
        from mirobody.collect.files.file_upload_manager import websocket_file_upload_manager

        # 🔧 Fix: Use original filename, or temporary filename if not provided
        display_filename = original_filename or temp_file_path.name
        display_file_size = original_file_size or (temp_file_path.stat().st_size if temp_file_path.exists() else 0)

        # Determine the user ID for genetic data ownership
        # Use target_user_id if provided (upload for others), otherwise use uploader's user_id
        data_owner_user_id = target_user_id or user_id

        # Every status below is written to the upload's th_files row.
        await FileDbService.rows_ready(message_id)

        # Pass file_key to loader for th_files updates
        loader = GeneticDataLoader(message_id, language, user_id, display_filename, display_file_size, file_key)

        if file_key:
            await loader.update_progress(0, 0, localize("genetic_initializing_loader", language, "load_genetic_data"))

        # The uploader may write on behalf of an authorised care-circle member.
        loaded_records = await loader.load_user_genetic_data(
            data_owner_user_id,
            str(temp_file_path),
            source_table_id=source_table_id,
        )

        # Update completion status in th_files
        if file_key:
            final_content = localize(
                "genetic_processing_complete_message",
                language,
                "load_genetic_data",
                records=loaded_records,
            )

            url_value = full_url or display_filename
            
            # Update th_files with completion status
            await FileDbService.update_file_content(
                file_key=file_key,
                updates={
                    "status": "completed",
                    "progress": 100,
                    "processed": True,
                    "raw": final_content,
                    "file_abstract": file_abstract or "",
                    "loaded_records": loaded_records,
                    "timestamp": datetime.now().isoformat(),
                    "success": True,
                    "type": "genetic",
                }
            )

            # Send completion status via WebSocket
            if message_id:
                try:
                    detailed_final_message = f"✅ Genetic data processing completed! Saved {loaded_records:,} records"
                    send_success = await websocket_file_upload_manager.send_message_by_message_id(message_id, {
                        "type": "upload_completed", "messageId": message_id, "status": "completed",
                        "progress": 100, "message": detailed_final_message, "file_type": "genetic",
                        "filename": display_filename, "success": True, "raw": final_content,
                        "url_thumb": url_value, "url_full": url_value, "file_key": file_key,
                        "file_size": display_file_size, "file_abstract": file_abstract or "",
                        "processing_stats": {"processed_records": loaded_records, "saved_records": loaded_records,
                                           "progress_percent": 100, "stage": "genetic_completed"},
                        "genetic_processing_final": True
                    })
                    if send_success:
                        await websocket_file_upload_manager.update_genetic_processing_complete(str(user_id), message_id)
                except Exception:
                    pass  # WebSocket failure doesn't affect results

        logger.info(f"Genetic processing completed: {loaded_records} records")

        # 🔧 Fix: Return correct original file information
        return {
            "success": True,
            "message": localize("genetic_file_received", language, "load_genetic_data"),
            "type": "genetic",
            "url_thumb": display_filename,  # Use original filename
            "full_url": display_filename,  # Use original filename
            "filename": display_filename,  # Add original filename
            "file_size": display_file_size,  # Add original file size
            "loaded_records": loaded_records,
            "file_key": file_key,  # Add file_key
        }

    except Exception as e:
        error_msg = "Genetic data processing failed"
        logger.error("genotype processing failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))

        # 🔧 Fix: Use original filename, or temporary filename if not provided
        display_filename = original_filename or temp_file_path.name
        display_file_size = original_file_size or (temp_file_path.stat().st_size if temp_file_path.exists() else 0)

        # Update failure status in th_files
        if file_key:
            try:
                failed_content = localize(
                    "genetic_processing_failed_message",
                    language,
                    "load_genetic_data",
                    error=str(e) if isinstance(e, NotAGenotypeExport) else error_msg,
                )

                # Update th_files with failure status
                await FileDbService.update_file_content(
                    file_key=file_key,
                    updates={
                        "status": "failed",
                        "progress": 0,
                        "processed": False,
                        "success": False,
                        "error": error_msg,
                        "raw": failed_content,
                        "timestamp": datetime.now().isoformat(),
                        "type": "genetic",
                    }
                )

                # Send failure status via WebSocket
                try:
                    if message_id:
                        try:
                            send_success = await websocket_file_upload_manager.send_message_by_message_id(
                                message_id,
                                {
                                    "type": "upload_error",
                                    "messageId": message_id,
                                    "status": "failed",
                                    "progress": 0,
                                    "message": failed_content,
                                    "file_type": "genetic",
                                    "filename": display_filename,
                                    "success": False,
                                    "raw": failed_content,
                                    "url_thumb": display_filename,
                                    "url_full": display_filename,
                                    "file_key": file_key,
                                    "error": error_msg,
                                },
                            )
                            if send_success:
                                logger.info("genotype failure status sent", extra={"message_id": message_id})
                            else:
                                logger.info(f"WebSocket failure status send failed (session not found): message_id={message_id}")
                        except Exception as ws_error:
                            logger.warning("genotype failure notification failed: error_type=%s", type(ws_error).__name__)
                    else:
                        logger.warning("WebSocket failure status update skipped: message_id is empty")

                except Exception as ws_error:
                    logger.warning("genotype websocket unavailable: error_type=%s", type(ws_error).__name__)

            except Exception as update_error:
                logger.error("genotype file status update failed: error_type=%s", type(update_error).__name__)

        return {
            "success": False,
            "message": error_msg,
            "type": "error",
            "filename": display_filename,  # Add original filename
            "file_size": display_file_size,  # Add original file size
            "file_key": file_key,  # Add file_key
        }
    finally:
        # Clean up temporary files
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.info(f"Deleted temporary file: {temp_file_path}")
            except Exception as ex:
                logger.error(f"Failed to delete temporary file: {str(ex)}")


# If running this file directly, execute all tests
