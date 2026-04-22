"""Adapters that normalise live publisher files into canonical DataFrames.

Scrapers call an adapter only when a live file is detected (i.e. when the
canonical fixture sheet is absent). Fixture tests never touch an adapter —
they continue to feed the scraper's direct-read path and validate the
canonical contract.
"""
