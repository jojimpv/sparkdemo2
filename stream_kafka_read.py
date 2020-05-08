import logging
import time
import pyspark
from dynaconf import settings
from pyspark.sql.functions import from_json, col

from schema import LCR_RESULT_SCHEMA

INBOUND_DIR = settings.INBOUND_DIR
LOG_LEVEL = logging.DEBUG

logging.basicConfig(datefmt='%T',
                    format='%(asctime)s %(name)s %(levelname)s %(message)s'
                    )
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)


def main(spark: pyspark.sql.SparkSession):
    try:
        sdf = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'lcr-events') \
            .option("startingOffsets", "earliest") \
            .load()

        sdf.select(from_json(col('value').cast("string"), schema=LCR_RESULT_SCHEMA).alias('data')) \
            .select('data.*')\
            .writeStream \
            .format('console') \
            .start()

        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == '__main__':
    logger.info('Creating Spark session..')
    t00 = time.time()
    spark_session = pyspark.sql.SparkSession.builder \
        .appName(settings.APPNAME_KAFKA) \
        .master(settings.MASTER) \
        .getOrCreate()
    t01 = time.time()
    logger.info(f'Spark session created in {t01-t00:.0f} seconds')
    main(spark_session)
    t02 = time.time()
    logger.info(f'Program completed in {t02 - t01:.0f} seconds')
