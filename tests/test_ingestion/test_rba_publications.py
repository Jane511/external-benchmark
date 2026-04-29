"""End-to-end-ish tests for RBA publication capture."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select

from ingestion.adapters.rba_publications_adapter import RbaFsrScraper
from src.db import AuditLog, create_engine_and_schema, make_session_factory


class _Response:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _Session:
    def __init__(self, pages: dict[str, str]) -> None:
        self.pages = pages
        self.headers = {}

    def get(self, url: str, timeout: int = 30):
        return _Response(self.pages[url])


def test_rba_publication_run_writes_pdf_metadata_and_audit(tmp_path: Path) -> None:
    engine = create_engine_and_schema(":memory:")
    session = _Session({
        "https://www.rba.gov.au/publications/fsr/": (
            '<a href="/publications/fsr/2026/mar/">March</a>'
        ),
        "https://www.rba.gov.au/publications/fsr/2026/mar/": (
            '<a href="/publications/fsr/2026/pdf/financial-stability-review-2026-03.pdf">'
            "Download PDF</a>"
        ),
    })

    def fake_urlretrieve(url: str, filename: str):
        Path(filename).write_bytes(b"%PDF fake")
        return filename, None

    scraper = RbaFsrScraper(
        cache_base=tmp_path,
        session=session,  # type: ignore[arg-type]
        audit_engine=engine,
    )
    with patch("ingestion.downloader.urlretrieve", side_effect=fake_urlretrieve):
        result = scraper.run(force_refresh=True)

    assert result.local_cached_file.endswith("RBA_FSR_March_2026.pdf")
    cached = tmp_path / "rba" / "RBA_FSR_March_2026.pdf"
    assert cached.exists()
    assert cached.with_suffix(".pdf.metadata.json").exists()

    factory = make_session_factory(engine)
    with factory() as db:
        rows = db.scalars(select(AuditLog)).all()
    assert len(rows) == 1
    assert rows[0].operation == "read_source_document"
    assert rows[0].entity_id == "rba_fsr"
    assert "file_hash" in rows[0].params_json
