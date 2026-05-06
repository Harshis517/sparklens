"""
SparkLens Pipeline — Unified Orchestrator
Merges all three modules into a single end-to-end pipeline.
"""

import logging
import time
from pyspark.sql import DataFrame, SparkSession
from .missing_duplicates import MissingDuplicatesHandler
from .outlier_detection import OutlierDetector
from .schema_profiler import SchemaProfiler

logger = logging.getLogger("SparkLens.Pipeline")


class SparkLensPipeline:
    """
    End-to-end data quality pipeline combining all three SparkLens modules.

    Execution order:
      1. SchemaProfiler  — validate schema and profile BEFORE cleaning
      2. MissingDuplicatesHandler — normalize nulls, impute, deduplicate
      3. OutlierDetector — detect/handle numeric outliers
      4. SchemaProfiler  — profile AFTER cleaning (quality score comparison)

    Parameters
    ----------
    config : dict
        Full pipeline configuration. See run_collisions.py for an example.
    spark : SparkSession
        Active SparkSession.
    """

    def __init__(self, config: dict, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.full_report = {}

    def run(self, df: DataFrame) -> DataFrame:
        """Execute the full SparkLens pipeline on a DataFrame."""
        t0 = time.time()
        logger.info("╔══════════════════════════════════════════╗")
        logger.info("║        SparkLens Pipeline Starting       ║")
        logger.info("╚══════════════════════════════════════════╝")

        # ── Phase 0: Pre-clean profile ───────────────────────────────
        pre_profiler = SchemaProfiler(
            expected_schema=self.config.get("expected_schema"),
            output_path=self.config.get("pre_profile_output"),
        )
        df = pre_profiler.fit_transform(df)
        pre_profiler.print_profile()
        self.full_report["pre_clean_profile"] = pre_profiler.get_report()

        # ── Phase 1: Missing Values & Duplicates ─────────────────────
        md_handler = MissingDuplicatesHandler(
            null_representations=self.config.get("null_representations"),
            drop_col_threshold=self.config.get("drop_col_threshold", 0.80),
            drop_row_threshold=self.config.get("drop_row_threshold"),
            strategies=self.config.get("imputation_strategies", {}),
            fill_values=self.config.get("fill_values", {}),
            subset_cols=self.config.get("dedup_subset_cols"),
        )
        df = md_handler.fit_transform(df)
        self.full_report["missing_duplicates"] = md_handler.get_report()

        # ── Phase 2: Outlier Detection ───────────────────────────────
        outlier_detector = OutlierDetector(
            numeric_cols=self.config.get("outlier_cols"),
            method=self.config.get("outlier_method", "iqr"),
            action=self.config.get("outlier_action", "flag"),
            iqr_multiplier=self.config.get("iqr_multiplier", 1.5),
            zscore_threshold=self.config.get("zscore_threshold", 3.0),
        )
        df = outlier_detector.fit_transform(df)
        self.full_report["outlier_detection"] = outlier_detector.get_report()

        # ── Phase 3: Post-clean profile ──────────────────────────────
        post_profiler = SchemaProfiler(
            output_path=self.config.get("post_profile_output"),
        )
        df = post_profiler.fit_transform(df)
        post_profiler.print_profile()
        self.full_report["post_clean_profile"] = post_profiler.get_report()

        elapsed = round(time.time() - t0, 2)
        self.full_report["pipeline_duration_seconds"] = elapsed

        logger.info("╔══════════════════════════════════════════╗")
        logger.info(f"║  SparkLens Pipeline Complete ({elapsed}s)  ║")
        logger.info("╚══════════════════════════════════════════╝")

        return df

    def get_full_report(self) -> dict:
        return self.full_report

    def print_summary(self):
        """Print a concise pipeline summary to stdout."""
        r = self.full_report
        pre = r.get("pre_clean_profile", {})
        post = r.get("post_clean_profile", {})
        md = r.get("missing_duplicates", {})
        od = r.get("outlier_detection", {})

        print("\n" + "═" * 60)
        print("  SparkLens Pipeline Summary")
        print("═" * 60)
        print(f"  Rows before : {md.get('row_count_before', 'N/A'):,}")
        print(f"  Rows after  : {md.get('row_count_after', 'N/A'):,}")
        print(f"  Duplicates removed  : {md.get('duplicate_rows_removed', 0):,}")
        print(f"  Columns dropped     : {md.get('dropped_columns', [])}")
        print(f"  Quality score before: {pre.get('quality_score', 'N/A')}/100")
        print(f"  Quality score after : {post.get('quality_score', 'N/A')}/100")
        print(f"  Outlier method      : {od.get('method', 'N/A')}")
        print(f"  Outlier action      : {od.get('action', 'N/A')}")
        print(f"  Pipeline duration   : {r.get('pipeline_duration_seconds', 'N/A')}s")
        print("═" * 60 + "\n")
