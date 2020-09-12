import click
import findspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, trim, StringType, concat_ws, split, unix_timestamp, from_unixtime


def init_spark_connection(appname, sparkmaster, minio_url,
                          minio_access_key, minio_secret_key):
    """ Init Spark connection and set hadoop configuration to read
    data from MINIO.

    Args:
        appname: spark application name.
        sparkmaster: spark master url.
        minio_url: an url to access to MINIO.
        minio_access_key: specific access key to MINIO.
        minio_secret_key: specific secret key to MINIO.

    Return:
         sc: spark connection object
    """
    findspark.init()
    sc = SparkSession \
        .builder \
        .appName(appname) \
        .master(sparkmaster) \
        .getOrCreate()

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.endpoint", minio_url)
    hadoop_conf.set("fs.s3a.access.key", minio_access_key)
    hadoop_conf.set("fs.s3a.secret.key", minio_secret_key)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.path.style.access", "True")
    return sc


def extract(sc, bucket_name, raw_data_path):
    """ Extract csv files from Minio.

    Args:
        sc: spark connection object.
        bucket_name: name of specific bucket in minio that contain data.
        raw_data_path: a path in bucket name that specifies data location.

    Return:
        df: raw dataframe.
    """

    return sc.read.json('s3a://' + os.path.os.path.join(bucket_name,
                                                        raw_data_path))


def transform(df):
    """ Transform dataframe to an acceptable form.

    Args:
        df: raw dataframe

    Return:
        df: processed dataframe
    """
    # todo: write the your code here
    # Remove user field
    df_flatten_one_level = df.select('message.*', 'kafka_consume_ts')
    df_flatten_one_level = df_flatten_one_level.drop('user')

    # Remove retweeted_status and quoted_status if they are available in JSON objects and add them to dataframe as new
    #  rows.
    df_retweeted_status = df_flatten_one_level \
        .where(df_flatten_one_level.retweeted_status.isNotNull()) \
        .select('retweeted_status.*') \
        .drop('user') \
        .withColumn('keyword', lit(None).cast(StringType())) \
        .withColumn('kafka_consume_ts', lit(None).cast(StringType()))

    df_quoted_status = df_flatten_one_level \
        .where(df_flatten_one_level.quoted_status.isNotNull()) \
        .select('quoted_status.*') \
        .drop('user') \
        .withColumn('keyword', lit(None).cast(StringType())) \
        .withColumn('kafka_consume_ts', lit(None).cast(StringType()))

    df_flatten_one_level = df_flatten_one_level.drop("retweeted_status", "quoted_status")

    common_unnested_columns = ['created_at', 'favorite_count', 'favorited', 'id', 'id_str', 'in_reply_to_screen_name',
                               'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id',
                               'in_reply_to_user_id_str', 'is_quote_status', 'keyword', 'lang', 'possibly_sensitive',
                               'quoted_status_id', 'quoted_status_id_str', 'retweet_count', 'retweeted', 'source',
                               'text',
                               'truncated', 'kafka_consume_ts']

    df_flatten_one_level = df_flatten_one_level.select(common_unnested_columns) \
        .unionAll(df_retweeted_status.select(common_unnested_columns)) \
        .unionAll(df_quoted_status.select(common_unnested_columns))

    # Remove duplicate tweets.
    df_without_duplicate = df_flatten_one_level.dropDuplicates(subset=['id'])

    # Remove space characters from text fields.
    for item in df_without_duplicate.dtypes:
        if item[1].startswith('string'):
            df_without_duplicate = df_without_duplicate.withColumn(item[0], trim(df_without_duplicate[item[0]]))

    # Convert created_at field to DateTime with (year-month-day) format.
    split_col = split(df_without_duplicate['created_at'], ' ')
    df_with_date = df_without_duplicate.withColumn('created_at_date',
                                                   concat_ws('-',
                                                             split_col.getItem(5),
                                                             split_col.getItem(1),
                                                             split_col.getItem(2)
                                                             )
                                                   )
    df_final = df_with_date.withColumn("created_at_date",
                                       from_unixtime(
                                           unix_timestamp(df_with_date.created_at_date, 'yyyy-MMM-dd'),
                                           'yyyy-MM-dd'
                                       )
                                       )

    return df_final


def load(df, bucket_name, processed_data_path, partition_by):
    """ Load clean dataframe to MINIO.

    Args:
        df: a processed dataframe.
        bucket_name: the name of specific bucket in minio that contain data.
        processed_data_path: a path in bucket name that
            specifies data location.

    Returns:
         Nothing!
    """
    # todo: change this function if
    # Partition dataframe based on created_at date.
    df.write.partitionBy(partition_by).mode("overwrite").format("csv") \
        .save('s3a://' + os.path.os.path.join(bucket_name,
                                              processed_data_path),
              header=True)


@click.command('ETL job')
@click.option('--appname', '-a', default='ETL Task', help='Spark app name')
@click.option('--sparkmaster', default='local',
              help='Spark master node address:port')
@click.option('--minio_url', default='http://localhost:9001',
              help='import a module')
@click.option('--minio_access_key', default='AKIAIOSFODNN7EXAMPLE')
@click.option('--minio_secret_key', default='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
@click.option('--bucket_name', default='data')
@click.option('--raw_data_path', default='raw/tweets')
@click.option('--processed_data_path', default='processed/tweets')
@click.option('--partition_by', default='created_at_date')
def main(appname, sparkmaster, minio_url,
         minio_access_key, minio_secret_key,
         bucket_name, raw_data_path, processed_data_path, partition_by):
    sc = init_spark_connection(appname, sparkmaster, minio_url,
                               minio_access_key, minio_secret_key)

    # extract data from MINIO
    df = extract(sc, bucket_name, raw_data_path)

    # transform data to desired form
    clean_df = transform(df)

    # load clean data to MINIO
    load(clean_df, bucket_name, processed_data_path, partition_by)


if __name__ == '__main__':
    main()
