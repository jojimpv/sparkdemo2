import logging
import time
import pyspark
from dynaconf import settings

from schema import LCR_SCHEMA

INBOUND_DIR = settings.INBOUND_DIR
LOG_LEVEL = logging.DEBUG

logging.basicConfig(datefmt='%T',
                    format='%(asctime)s %(name)s %(levelname)s %(message)s'
                    )
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)


def main(spark):
    try:
        sdf = spark.readStream.csv(INBOUND_DIR, schema=LCR_SCHEMA)
        query = sdf.groupBy('borough')\
            .agg({'value': 'sum'})\
            .withColumnRenamed('sum(value)', 'count').orderBy('count', ascending=False)

        query.writeStream \
            .format('console') \
            .outputMode('complete') \
            .start()
        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == '__main__':
    logger.info('Creating Spark session..')
    t00 = time.time()
    spark_session = pyspark.sql.SparkSession.builder \
        .appName(settings.APPNAME) \
        .master(settings.MASTER) \
        .getOrCreate()
    t01 = time.time()
    logger.info(f'Spark session created in {t01-t00:.0f} seconds')
    main(spark_session)
    t02 = time.time()
    logger.info(f'Program completed in {t02 - t01:.0f} seconds')
