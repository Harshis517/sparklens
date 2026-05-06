"""
SparkLens — A Reusable Spark-Based Library for Data Cleaning and Quality Assessment
Team: Utkarsh Arora (ua2152), Harshita Singh (hs5412), Debdeep Naha (dn2491)
Big Data Analytics Symposium, Spring 2026
"""

from .missing_duplicates import MissingDuplicatesHandler
from .outlier_detection import OutlierDetector
from .schema_profiler import SchemaProfiler
from .pipeline import SparkLensPipeline

__version__ = "1.0.0"
__all__ = [
    "MissingDuplicatesHandler",
    "OutlierDetector",
    "SchemaProfiler",
    "SparkLensPipeline",
]
