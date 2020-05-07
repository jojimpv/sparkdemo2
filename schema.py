from pyspark.sql.types import StructType, StringType, IntegerType

# lsoa_code,borough,major_category,minor_category,value,year,month
LCR_SCHEMA = StructType()\
    .add('lsoa_code', StringType()) \
    .add('borough', StringType()) \
    .add('major_category', StringType()) \
    .add('minor_category', StringType()) \
    .add('value', IntegerType()) \
    .add('year', IntegerType()) \
    .add('month', IntegerType())
