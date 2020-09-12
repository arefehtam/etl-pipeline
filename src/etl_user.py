import click
import findspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, split, trim, unix_timestamp, from_unixtime


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
    # Select the following fields; id, id_str, name, screen_name, location, description, url, protected,
    # followers_count, friends_count, listed_count, created_at, favourites_count, statuses_count, lang,
    # profile_image_url_https, timestamp.
    df_select = df.select("message.id", "message.id_str", "message.name", "message.screen_name", "message.location",
                          "message.description", "message.url", "message.protected", "message.followers_count",
                          "message.friends_count", "message.listed_count", "message.created_at",
                          "message.favourites_count",
                          "message.statuses_count", "message.status.lang", "message.profile_image_url_https",
                          "timestamp")

    # Remove duplicate users
    df_without_duplicate = df_select.dropDuplicates(subset=['id'])

    # Remove space characters from description, name, location, and URL fields.
    df_without_space = df_without_duplicate.withColumn("name", trim(df_without_duplicate.name))
    df_without_space = df_without_space.withColumn("description", trim(df_without_space.description))
    df_without_space = df_without_space.withColumn("location", trim(df_without_space.location))
    df_without_space = df_without_space.withColumn("url", trim(df_without_space.url))

    # Convert created_at field to DateTime with (year-month-day) format.
    split_col = split(df_select['created_at'], ' ')
    df_without_space = df_without_space.withColumn('created_at_date',
                                                   concat_ws('-',
                                                             split_col.getItem(5),
                                                             split_col.getItem(1),
                                                             split_col.getItem(2)
                                                             )
                                                   )
    df_final = df_without_space.withColumn("created_at_date",
                                           from_unixtime(
                                               unix_timestamp(df_without_space.created_at_date, 'yyyy-MMM-dd'),
                                               'yyyy-MM-dd'
                                           )
                                           )

    return df_final


def load(df, bucket_name, processed_data_path):
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
    df.write.csv('s3a://' + os.path.os.path.join(bucket_name,
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
@click.option('--raw_data_path', default='raw/users')
@click.option('--processed_data_path', default='processed/users')
def main(appname, sparkmaster, minio_url,
         minio_access_key, minio_secret_key,
         bucket_name, raw_data_path, processed_data_path):
    sc = init_spark_connection(appname, sparkmaster, minio_url,
                               minio_access_key, minio_secret_key)

    # extract data from MINIO
    df = extract(sc, bucket_name, raw_data_path)

    # transform data to desired form
    clean_df = transform(df)

    # load clean data to MINIO
    load(clean_df, bucket_name, processed_data_path)


if __name__ == '__main__':
    main()
