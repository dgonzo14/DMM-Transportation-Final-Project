#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfl_schemas.py
Spark schemas for all four TfL bronze data types, plus a generic
batch reader that constructs the correct schema for a given data type.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, ArrayType,
)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

def arrivals_schema() -> StructType:
    timing = StructType([
        StructField("countdownServerAdjustment", StringType(), True),
        StructField("source",   StringType(), True),
        StructField("insert",   StringType(), True),
        StructField("read",     StringType(), True),
        StructField("sent",     StringType(), True),
        StructField("received", StringType(), True),
    ])
    return StructType([
        StructField("id",                  StringType(),  True),
        StructField("operationType",       IntegerType(), True),
        StructField("vehicleId",           StringType(),  True),
        StructField("naptanId",            StringType(),  True),
        StructField("stationName",         StringType(),  True),
        StructField("lineId",              StringType(),  True),
        StructField("lineName",            StringType(),  True),
        StructField("platformName",        StringType(),  True),
        StructField("direction",           StringType(),  True),
        StructField("destinationNaptanId", StringType(),  True),
        StructField("destinationName",     StringType(),  True),
        StructField("timestamp",           StringType(),  True),
        StructField("timeToStation",       IntegerType(), True),
        StructField("currentLocation",     StringType(),  True),
        StructField("towards",             StringType(),  True),
        StructField("expectedArrival",     StringType(),  True),
        StructField("timeToLive",          StringType(),  True),
        StructField("modeName",            StringType(),  True),
        StructField("timing",              timing,        True),
    ])


def crowding_schema() -> StructType:
    return StructType([
        StructField("dataAvailable",        BooleanType(), True),
        StructField("percentageOfBaseline", DoubleType(),  True),
        StructField("timeUtc",              StringType(),  True),
        StructField("timeLocal",            StringType(),  True),
    ])


def status_schema() -> StructType:
    line_status = StructType([
        StructField("id",                        IntegerType(), True),
        StructField("lineId",                    StringType(),  True),
        StructField("statusSeverity",            IntegerType(), True),
        StructField("statusSeverityDescription", StringType(),  True),
        StructField("reason",                    StringType(),  True),
        StructField("created",                   StringType(),  True),
    ])
    return StructType([
        StructField("id",           StringType(),           True),
        StructField("name",         StringType(),           True),
        StructField("modeName",     StringType(),           True),
        StructField("created",      StringType(),           True),
        StructField("modified",     StringType(),           True),
        StructField("lineStatuses", ArrayType(line_status), True),
    ])


def lift_disruptions_schema() -> StructType:
    return StructType([
        StructField("stationUniqueId",        StringType(),            True),
        StructField("disruptedLiftUniqueIds", ArrayType(StringType()), True),
        StructField("message",                StringType(),            True),
    ])


SCHEMA_REGISTRY = {
    "arrivals":         arrivals_schema,
    "crowding":         crowding_schema,
    "status":           status_schema,
    "lift_disruptions": lift_disruptions_schema,
}


# ---------------------------------------------------------------------------
# Generic batch reader
# ---------------------------------------------------------------------------

def read_bronze(spark: SparkSession, data_type: str, paths: list[str]) -> DataFrame:
    """
    Reads bronze JSON files for a given data type from explicit paths.
    Schema is looked up from SCHEMA_REGISTRY.
    """
    if data_type not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown data_type {data_type!r}. "
                         f"Valid: {list(SCHEMA_REGISTRY)}")
    return (
        spark.read
        .format("json")
        .schema(SCHEMA_REGISTRY[data_type]())
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .load(paths)
    )


def stream_bronze(spark: SparkSession, data_type: str, base_path: str,
                  max_files_per_trigger: int = 50) -> DataFrame:
    """
    Returns a streaming DataFrame for a given data type.
    base_path should be the top-level data type prefix,
    e.g. s3a://bucket/bronze/tfl/arrivals/
    """
    if data_type not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown data_type {data_type!r}.")
    return (
        spark.readStream
        .format("json")
        .schema(SCHEMA_REGISTRY[data_type]())
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(base_path)
    )