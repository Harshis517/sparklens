"""
SparkLens Module 1: Missing Values & Duplicates Handler
Author: Debdeep Naha (dn2491)

Handles:
  - Null normalization (converts "", "N/A", "null", "None", "Unknown" to Spark null)
  - Column-level null percentage reporting
  - Drop columns/rows exceeding null threshold
  - Multi-strategy imputation: mean, median, mode, constant
  - Duplicate detection and removal
"""

import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType

logger = logging.getLogger("SparkLens.MissingDuplicates")


class MissingDuplicatesHandler:
    """
    Handles missing value normalization, imputation, and duplicate removal.

    Parameters
    ----------
    null_representations : list
        String values to treat as null (default: ["N/A", "null", "None", "", "Unknown", "n/a"])
    drop_col_threshold : float
        Drop columns where null fraction exceeds this value (default: 0.80)
    drop_row_threshold : int or None
        Drop rows with more than this many nulls across all columns (None = skip)
    strategies : dict
        Per-column imputation strategy. Keys are column names, values are one of:
        "mean", "median", "mode", "constant". Falls back to "mode" for string cols.
    fill_values : dict
        Constant fill values per column, used when strategy is "constant".
    subset_cols : list or None
        Deduplicate based only on these columns (None = all columns).
    """

    DEFAULT_NULL_REPRS = ["N/A", "null", "None", "", "Unknown", "n/a", "NA", "NaN"]

    def __init__(
        self,
        null_representations=None,
        drop_col_threshold=0.80,
        drop_row_threshold=None,
        strategies=None,
        fill_values=None,
        subset_cols=None,
    ):
        self.null_representations = null_representations or self.DEFAULT_NULL_REPRS
        self.drop_col_threshold = drop_col_threshold
        self.drop_row_threshold = drop_row_threshold
        self.strategies = strategies or {}
        self.fill_values = fill_values or {}
        self.subset_cols = subset_cols
        self.report = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fit_transform(self, df: DataFrame) -> DataFrame:
        """Run the full missing value + deduplication pipeline."""
        logger.info("=== SparkLens: Missing Values & Duplicates Module ===")

        # Step 1: Normalize null representations
        df = self._normalize_nulls(df)

        # Step 2: Audit null counts before cleaning
        self.report["null_counts_before"] = self._audit_nulls(df)
        total = df.count()
        self.report["row_count_before"] = total
        logger.info(f"Row count before cleaning: {total:,}")

        # Step 3: Drop high-null columns
        df = self._drop_high_null_columns(df)

        # Step 4: Drop high-null rows (optional)
        if self.drop_row_threshold is not None:
            df = self._drop_high_null_rows(df)

        # Step 5: Impute remaining nulls
        df = self._impute(df)

        # Step 6: Deduplicate
        dup_before = total - df.dropDuplicates(self.subset_cols).count() if self.subset_cols else total - df.dropDuplicates().count()
        df_dedup = df.dropDuplicates(self.subset_cols) if self.subset_cols else df.dropDuplicates()
        self.report["duplicate_rows_removed"] = dup_before
        logger.info(f"Duplicate rows removed: {dup_before:,}")

        # Final audit
        self.report["null_counts_after"] = self._audit_nulls(df_dedup)
        self.report["row_count_after"] = df_dedup.count()
        logger.info(f"Row count after cleaning: {self.report['row_count_after']:,}")

        return df_dedup

    def get_report(self) -> dict:
        return self.report

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _normalize_nulls(self, df: DataFrame) -> DataFrame:
        """Replace all known null representations with actual Spark nulls."""
        from pyspark.sql.types import StringType, NumericType
        logger.info(f"Normalizing null representations: {self.null_representations}")
        for field in df.schema.fields:
            col_name = field.name
            if isinstance(field.dataType, StringType):
                # String columns: direct isin check
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isin(self.null_representations), None)
                     .otherwise(F.col(col_name)),
                )
            elif not isinstance(field.dataType, NumericType):
                # Other non-numeric: cast to string, check, cast back
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).cast("string").isin(self.null_representations), None)
                     .otherwise(F.col(col_name)),
                )
            # Numeric columns: skip string-based null normalization (they use Spark nulls already)
        return df

    def _audit_nulls(self, df: DataFrame) -> dict:
        """Return {column: null_count} for every column."""
        total = df.count()
        null_counts = {}
        exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]
        row = df.select(exprs).collect()[0]
        for c in df.columns:
            null_counts[c] = {"count": row[c], "pct": round(row[c] / total * 100, 2) if total > 0 else 0}
        return null_counts

    def _drop_high_null_columns(self, df: DataFrame) -> DataFrame:
        """Drop columns whose null fraction exceeds drop_col_threshold."""
        total = df.count()
        exprs = [(F.count(F.when(F.col(c).isNull(), 1)) / F.lit(total)).alias(c) for c in df.columns]
        fractions = df.select(exprs).collect()[0].asDict()
        to_drop = [c for c, v in fractions.items() if v > self.drop_col_threshold]
        if to_drop:
            logger.info(f"Dropping {len(to_drop)} columns exceeding {self.drop_col_threshold*100:.0f}% null threshold: {to_drop}")
            self.report["dropped_columns"] = to_drop
            df = df.drop(*to_drop)
        else:
            self.report["dropped_columns"] = []
        return df

    def _drop_high_null_rows(self, df: DataFrame) -> DataFrame:
        """Drop rows with more than drop_row_threshold null values."""
        null_count_expr = sum(F.when(F.col(c).isNull(), 1).otherwise(0) for c in df.columns)
        df = df.filter(null_count_expr <= self.drop_row_threshold)
        return df

    def _impute(self, df: DataFrame) -> DataFrame:
        """Impute nulls column by column using configured strategies."""
        numeric_cols = {
            f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)
        }

        for col_name in df.columns:
            if col_name not in self.strategies:
                # Default: mode for string, median for numeric
                strategy = "median" if col_name in numeric_cols else "mode"
            else:
                strategy = self.strategies[col_name]

            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count == 0:
                continue

            try:
                if strategy == "mean" and col_name in numeric_cols:
                    val = df.select(F.mean(F.col(col_name))).collect()[0][0]
                elif strategy == "median" and col_name in numeric_cols:
                    val = df.approxQuantile(col_name, [0.5], 0.01)[0]
                elif strategy == "mode":
                    mode_row = (
                        df.filter(F.col(col_name).isNotNull())
                        .groupBy(col_name)
                        .count()
                        .orderBy(F.col("count").desc())
                        .first()
                    )
                    val = mode_row[col_name] if mode_row else None
                elif strategy == "constant":
                    val = self.fill_values.get(col_name)
                else:
                    val = None

                if val is not None:
                    df = df.fillna({col_name: val})
                    logger.debug(f"Imputed '{col_name}' with {strategy}={val} ({null_count} nulls)")
            except Exception as e:
                logger.warning(f"Could not impute '{col_name}' with {strategy}: {e}")

        return df
