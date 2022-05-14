import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.schemas.epl_data_schema import EPL_SCHEMA

@pytest.fixture
def schema_structure():
  return StructType([
    StructField("HomeTeam",StringType(),False),
    StructField("AwayTeam",StringType(),False),
    StructField("HS",IntegerType(),False),
    StructField("AS",IntegerType(),False),
    StructField("FTHG",IntegerType(),False),
    StructField("FTAG",IntegerType(),False),
    StructField("FTR",StringType(),False)
  ])

def test_epl_schema_structure(schema_structure):
  assert EPL_SCHEMA == schema_structure