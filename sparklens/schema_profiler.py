"""
SparkLens Module 3: Schema Validation & Column Profiling
Author: Utkarsh Arora (ua2152)

Handles:
  - Expected schema validation (column names, data types, nullable constraints)
  - Per-column profiling: null count, distinct count, min/max, mean/stddev for numeric cols
  - Data quality score (composite metric)
  - Optional JSON/CSV report export
"""

import json
import logging
import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

logger = logging.getLogger("SparkLens.SchemaProfiler")


class SchemaProfiler:
    """
    Validates schema and generates a column-level data quality profile.

    Parameters
    ----------
    expected_schema : dict or None
        {
          "columns": {"col_name": "expected_spark_type_string", ...},
          "nullable": {"col_name": True/False, ...}   # optional
        }
        If None, only profiling is performed (no validation).
    output_path : str or None
        If provided, the profile report is written as JSON to this path.
    """

    def __init__(self, expected_schema=None, output_path=None):
        self.expected_schema = expected_schema or {}
        self.output_path = output_path
        self.report = {}
        self.validation_issues = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fit_transform(self, df: DataFrame) -> DataFrame:
        """Validate schema and generate column profile. Returns df unchanged."""
        logger.info("=== SparkLens: Schema Validation & Column Profiling Module ===")

        # Schema validation
        if self.expected_schema:
            self.validation_issues = self._validate_schema(df)
            if self.validation_issues:
                logger.warning(f"Schema validation found {len(self.validation_issues)} issues:")
                for issue in self.validation_issues:
                    logger.warning(f"  ⚠ {issue}")
            else:
                logger.info("Schema validation passed — no issues found.")

        # Column profiling (single aggregation pass)
        self.report["schema_issues"] = self.validation_issues
        self.report["column_profiles"] = self._profile_columns(df)
        self.report["quality_score"] = self._compute_quality_score(df, self.report["column_profiles"])
        self.report["total_rows"] = df.count()
        self.report["total_columns"] = len(df.columns)

        logger.info(f"Overall Data Quality Score: {self.report['quality_score']:.1f} / 100")

        # Export report
        if self.output_path:
            self._export_report()

        return df  # schema profiling is non-destructive

    def get_report(self) -> dict:
        return self.report

    def print_profile(self):
        """Pretty-print the column profile to stdout."""
        print(f"\n{'='*80}")
        print(f"  SparkLens Column Profile Report")
        print(f"  Rows: {self.report.get('total_rows','N/A'):,}  |  "
              f"Columns: {self.report.get('total_columns','N/A')}  |  "
              f"Quality Score: {self.report.get('quality_score', 0):.1f}/100")
        print(f"{'='*80}")
        for col, p in self.report.get("column_profiles", {}).items():
            print(f"\n  [{col}]  type={p['data_type']}")
            print(f"    nulls={p['null_count']:,} ({p['null_pct']:.1f}%)  "
                  f"distinct={p['distinct_count']:,}  "
                  f"completeness={p['completeness_pct']:.1f}%")
            if p.get("min") is not None:
                print(f"    min={p['min']}  max={p['max']}  "
                      f"mean={p.get('mean','N/A')}  stddev={p.get('stddev','N/A')}")
        if self.validation_issues:
            print(f"\n  Schema Issues:")
            for issue in self.validation_issues:
                print(f"    ⚠ {issue}")
        print(f"\n{'='*80}\n")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_schema(self, df: DataFrame) -> list:
        """Compare actual schema to expected schema, return list of issue strings."""
        issues = []
        actual_types = {f.name: (str(f.dataType), f.nullable) for f in df.schema.fields}
        expected_cols = self.expected_schema.get("columns", {})
        nullable_map = self.expected_schema.get("nullable", {})

        for col_name, exp_type in expected_cols.items():
            if col_name not in actual_types:
                issues.append(f"MISSING column: '{col_name}' (expected type={exp_type})")
            else:
                act_type, act_nullable = actual_types[col_name]
                if exp_type.lower() not in act_type.lower():
                    issues.append(
                        f"TYPE MISMATCH '{col_name}': expected={exp_type}, got={act_type}"
                    )

        for col_name, should_be_nullable in nullable_map.items():
            if col_name not in actual_types:
                continue
            null_count = df.filter(F.col(col_name).isNull()).count()
            if not should_be_nullable and null_count > 0:
                issues.append(
                    f"NON-NULLABLE VIOLATION '{col_name}': {null_count:,} null values found"
                )

        # Warn about extra columns not in expected schema
        for col_name in actual_types:
            if col_name not in expected_cols and expected_cols:
                issues.append(f"UNEXPECTED column: '{col_name}' (not in expected schema)")

        return issues

    def _profile_columns(self, df: DataFrame) -> dict:
        """Generate per-column statistics in a single distributed pass."""
        total = df.count()
        profiles = {}

        for field in df.schema.fields:
            col_name = field.name
            is_numeric = isinstance(field.dataType, NumericType)

            # Base stats
            base_row = df.select(
                F.count(F.when(F.col(col_name).isNull(), 1)).alias("nulls"),
                F.countDistinct(F.col(col_name)).alias("distinct"),
            ).collect()[0]

            null_count = base_row["nulls"]
            distinct_count = base_row["distinct"]
            completeness = round((1 - null_count / total) * 100, 2) if total > 0 else 0

            profile = {
                "data_type": str(field.dataType),
                "nullable": field.nullable,
                "null_count": null_count,
                "null_pct": round(null_count / total * 100, 2) if total > 0 else 0,
                "distinct_count": distinct_count,
                "completeness_pct": completeness,
                "min": None,
                "max": None,
                "mean": None,
                "stddev": None,
                "top_values": None,
            }

            if is_numeric:
                try:
                    stats = df.select(
                        F.min(F.col(col_name)).alias("min"),
                        F.max(F.col(col_name)).alias("max"),
                        F.mean(F.col(col_name)).alias("mean"),
                        F.stddev(F.col(col_name)).alias("stddev"),
                    ).collect()[0]
                    profile["min"] = stats["min"]
                    profile["max"] = stats["max"]
                    profile["mean"] = round(stats["mean"], 4) if stats["mean"] else None
                    profile["stddev"] = round(stats["stddev"], 4) if stats["stddev"] else None
                except Exception:
                    pass
            else:
                # Top 3 most frequent non-null values for categorical columns
                try:
                    top = (
                        df.filter(F.col(col_name).isNotNull())
                        .groupBy(col_name)
                        .count()
                        .orderBy(F.col("count").desc())
                        .limit(3)
                        .collect()
                    )
                    profile["top_values"] = [
                        {"value": str(r[col_name]), "count": r["count"]} for r in top
                    ]
                except Exception:
                    pass

            profiles[col_name] = profile

        return profiles

    def _compute_quality_score(self, df: DataFrame, profiles: dict) -> float:
        """
        Composite quality score (0–100):
          - Completeness  (50 pts): average non-null percentage across columns
          - Uniqueness    (20 pts): fraction of columns with < 10% duplicate rate
          - Validity      (30 pts): absence of schema issues
        """
        if not profiles:
            return 0.0

        # Completeness
        avg_completeness = sum(p["completeness_pct"] for p in profiles.values()) / len(profiles)
        completeness_score = avg_completeness * 0.50

        # Uniqueness: reward columns with reasonable distinct count ratios
        total = self.report.get("total_rows", 1)
        unique_cols = sum(1 for p in profiles.values() if p["distinct_count"] / max(total, 1) > 0.90)
        uniqueness_score = (unique_cols / len(profiles)) * 20

        # Validity: deduct per schema issue
        issue_penalty = min(len(self.validation_issues) * 2, 30)
        validity_score = 30 - issue_penalty

        return round(completeness_score + uniqueness_score + validity_score, 2)

    def _export_report(self):
        """Write report as JSON to output_path."""
        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            with open(self.output_path, "w") as f:
                json.dump(self.report, f, indent=2, default=str)
            logger.info(f"Profile report written to {self.output_path}")
        except Exception as e:
            logger.warning(f"Could not write report: {e}")
