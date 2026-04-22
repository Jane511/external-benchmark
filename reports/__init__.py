"""Committee-reporting layer for the External Benchmark Engine.

Phase 1 (this package):
  - benchmark_report.BenchmarkCalibrationReport  top-level MRC / Credit Committee report
  - docx_helpers                                 shared python-docx primitives
                                                 (page setup, tables, headings) used
                                                 by both governance.export_to_docx and
                                                 BenchmarkCalibrationReport.to_docx

Future (blocked on industry-analysis sibling project):
  - environment_report.py        Report 2
  - combined_dashboard.py        Report 3
"""
# benchmark_report is imported lazily by consumers to avoid paying the
# python-docx import cost when only DOCX helpers are needed.
__all__ = ["BenchmarkCalibrationReport"]


def __getattr__(name):
    if name == "BenchmarkCalibrationReport":
        from reports.benchmark_report import BenchmarkCalibrationReport
        return BenchmarkCalibrationReport
    raise AttributeError(f"module 'reports' has no attribute {name!r}")
