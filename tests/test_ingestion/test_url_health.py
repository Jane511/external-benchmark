"""URL health checks — assert the downloader configs point to valid URLs.

Does NOT actually download (we don't want CI hitting external sites).
Asserts each URL parses correctly and that secondary URLs are present
where the brief specified them, plus a regression set that none of the
known-broken paths leak back in.
"""
from __future__ import annotations

import urllib.parse

import pytest

from scripts.download_sources.non_bank_downloader import _LENDERS


def test_every_lender_has_well_formed_url() -> None:
    for lender, cfg in _LENDERS.items():
        primary = cfg["ir_url"]
        parsed = urllib.parse.urlparse(primary)
        assert parsed.scheme in {"http", "https"}, f"{lender}: bad scheme"
        assert parsed.netloc, f"{lender}: missing netloc"


def test_lenders_with_known_split_paths_have_secondary() -> None:
    """Lenders whose IR is split across multiple paths must declare a
    secondary URL so the downloader can fall through gracefully.
    """
    requires_secondary = {
        "liberty", "resimac", "moneyme", "plenti", "wisr", "metrics_credit",
    }
    for lender in requires_secondary:
        cfg = _LENDERS[lender]
        assert cfg.get("ir_url_secondary"), (
            f"{lender}: brief specifies a secondary URL but config is missing it"
        )


def test_no_url_uses_known_broken_paths() -> None:
    """Regression — none of the known-broken paths from the URL audit
    should appear in any lender's primary URL.
    """
    broken_substrings = [
        "/about/investor-centre",          # Judo old path
        "libertyfinancial.com.au",          # Liberty old domain
        "/about/investor-relations",        # Resimac old path
        "/about-us/investor-centre/",       # MoneyMe old path
        "/news-and-insights/",              # Metrics old path
    ]
    for lender, cfg in _LENDERS.items():
        primary = cfg["ir_url"]
        for broken in broken_substrings:
            assert broken not in primary, (
                f"{lender}: URL {primary} contains known-broken path {broken!r}"
            )

    # Path-only check — these are valid path fragments on other (correct)
    # domains, so we only flag them if they appear on the legacy lender's
    # primary domain. E.g. "/investors/" on plenti.com.au was retired,
    # but other lenders may legitimately use that path on a different host.
    legacy_path_by_lender = {
        "plenti":   "/investors/",
        "wisr":     "/investor-centre/",
        "qualitas": "https://www.qualitas.com.au/investor-centre/",
    }
    for lender, broken in legacy_path_by_lender.items():
        primary = _LENDERS[lender]["ir_url"]
        assert broken not in primary, (
            f"{lender}: URL {primary} regressed to legacy path {broken!r}"
        )


def test_every_lender_carries_a_manual_hint() -> None:
    for lender, cfg in _LENDERS.items():
        hint = cfg.get("manual_hint")
        assert hint and isinstance(hint, str) and len(hint) > 20, (
            f"{lender}: manual_hint must be a non-trivial string for the gate file"
        )


def test_secondary_url_when_present_is_well_formed() -> None:
    for lender, cfg in _LENDERS.items():
        secondary = cfg.get("ir_url_secondary")
        if not secondary:
            continue
        parsed = urllib.parse.urlparse(secondary)
        assert parsed.scheme in {"http", "https"}, (
            f"{lender}: secondary URL has bad scheme: {secondary}"
        )
        assert parsed.netloc, f"{lender}: secondary URL missing netloc"


@pytest.mark.parametrize("lender", sorted(_LENDERS))
def test_file_pattern_and_fallback_keywords_present(lender: str) -> None:
    cfg = _LENDERS[lender]
    assert cfg.get("file_pattern"), f"{lender}: file_pattern missing"
    assert cfg.get("fallback_keywords"), f"{lender}: fallback_keywords missing"
    assert cfg.get("fallback_ext"), f"{lender}: fallback_ext missing"


def test_pepper_fallback_excludes_green_bond_frameworks() -> None:
    cfg = _LENDERS["pepper"]
    excludes = " ".join(cfg.get("exclude_keywords", [])).lower()
    assert "green bond" in excludes
    assert "framework" in excludes
    assert "investor" not in [k.lower() for k in cfg["fallback_keywords"]]
