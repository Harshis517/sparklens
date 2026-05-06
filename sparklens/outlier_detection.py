"""
SparkLens Module 2: Outlier Detection & Handling
Author: Harshita Singh (hs5412)

Handles:
  - IQR-based outlier detection (Tukey fences)
  - Z-score-based outlier detection
  - Actions: flag (add boolean column), cap (Winsorize), remove (filter rows)
  - Outlier summary report
"""

import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType

logger = logging.getLogger("SparkLens.OutlierDetection")


class OutlierDetector:
    """
    Detects and handles outliers in numeric columns.

    Parameters
    ----------
    numeric_cols : list or None
        Columns to check. If None, all numeric columns are used.
    method : str
        "iqr" (default) or "zscore"
    action : str
        "flag"   — adds a boolean column <col>_is_outlier
        "cap"    — Winsorizes values to [lower, upper]
        "remove" — drops rows where any selected column is an outlier
    iqr_multiplier : float
        Fence multiplier for IQR method (default: 1.5)
    zscore_threshold : float
        Absolute Z-score threshold (default: 3.0)
    relative_error : float
        Relative error for approxQuantile (default: 0.01)
    """

    def __init__(
        self,
        numeric_cols=None,
        method="iqr",
        action="flag",
        iqr_multiplier=1.5,
        zscore_threshold=3.0,
        relative_error=0.01,
    ):
        assert method in ("iqr", "zscore"), "method must be 'iqr' or 'zscore'"
        assert action in ("flag", "cap", "remove"), "action must be 'flag', 'cap', or 'remove'"
        self.numeric_cols = numeric_cols
        self.method = method
        self.action = action
        self.iqr_multiplier = iqr_multiplier
        self.zscore_threshold = zscore_threshold
        self.relative_error = relative_error
        self.report = {}
        self._bounds = {}  # {col: (lower, upper)}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fit_transform(self, df: DataFrame) -> DataFrame:
        """Detect and handle outliers across all configured numeric columns."""
        logger.info(f"=== SparkLens: Outlier Detection Module (method={self.method}, action={self.action}) ===")

        # Resolve numeric columns
        cols_to_check = self.numeric_cols or [
            f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)
        ]
        # Only process columns actually present in the dataframe
        cols_to_check = [c for c in cols_to_check if c in df.columns]

        self.report["method"] = self.method
        self.report["action"] = self.action
        self.report["columns_checked"] = cols_to_check
        self.report["outlier_summary"] = {}

        outlier_filters = []  # for "remove" action — accumulate conditions

        for col_name in cols_to_check:
            try:
                df, lower, upper, outlier_count = self._process_column(df, col_name)
                self._bounds[col_name] = (lower, upper)
                self.report["outlier_summary"][col_name] = {
                    "lower_bound": round(lower, 4) if lower is not None else None,
                    "upper_bound": round(upper, 4) if upper is not None else None,
                    "outlier_count": outlier_count,
                }
                logger.info(
                    f"  {col_name}: bounds=[{lower:.4f}, {upper:.4f}], "
                    f"outliers={outlier_count:,}"
                )
                if self.action == "remove" and lower is not None:
                    flag_col = f"{col_name}_is_outlier"
                    outlier_filters.append(F.col(flag_col) == False)
            except Exception as e:
                logger.warning(f"  Skipping '{col_name}': {e}")

        # Apply removal in one pass (avoid repeated scans)
        if self.action == "remove" and outlier_filters:
            combined = outlier_filters[0]
            for f_expr in outlier_filters[1:]:
                combined = combined & f_expr
            before = df.count()
            df = df.filter(combined)
            # Drop the temporary flag columns
            flag_cols = [f"{c}_is_outlier" for c in cols_to_check if f"{c}_is_outlier" in df.columns]
            df = df.drop(*flag_cols)
            after = df.count()
            self.report["rows_removed"] = before - after
            logger.info(f"Rows removed by outlier filter: {before - after:,}")

        return df

    def get_report(self) -> dict:
        return self.report

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _process_column(self, df: DataFrame, col_name: str):
        """Process a single column: compute bounds, flag, and optionally act."""
        if self.method == "iqr":
            lower, upper = self._iqr_bounds(df, col_name)
        else:
            lower, upper = self._zscore_bounds(df, col_name)

        is_outlier = (F.col(col_name) < lower) | (F.col(col_name) > upper)
        outlier_count = df.filter(is_outlier).count()

        if self.action == "flag" or self.action == "remove":
            df = df.withColumn(f"{col_name}_is_outlier", is_outlier)

        elif self.action == "cap":
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) < lower, lower)
                 .when(F.col(col_name) > upper, upper)
                 .otherwise(F.col(col_name))
            )

        return df, lower, upper, outlier_count

    def _iqr_bounds(self, df: DataFrame, col_name: str):
        """Compute IQR-based Tukey fences."""
        q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], self.relative_error)
        iqr = q3 - q1
        lower = q1 - self.iqr_multiplier * iqr
        upper = q3 + self.iqr_multiplier * iqr
        return lower, upper

    def _zscore_bounds(self, df: DataFrame, col_name: str):
        """Compute Z-score-based bounds using mean ± threshold * stddev."""
        stats = df.select(
            F.mean(F.col(col_name)).alias("mean"),
            F.stddev(F.col(col_name)).alias("stddev"),
        ).collect()[0]
        mean_v, std_v = stats["mean"], stats["stddev"]
        if std_v is None or std_v == 0:
            raise ValueError(f"Zero or null stddev for column '{col_name}'")
        # Add z-score column for reference (dropped after use)
        df = df.withColumn(f"_z_{col_name}", (F.col(col_name) - mean_v) / std_v)
        lower = mean_v - self.zscore_threshold * std_v
        upper = mean_v + self.zscore_threshold * std_v
        df = df.drop(f"_z_{col_name}")
        return lower, upper
