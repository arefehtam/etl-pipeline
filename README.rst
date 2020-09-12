===========
etl-pipeline
===========


Reading JSON files from Minio, process them using Spark, write them back to Minio as CSV files


Getting Started
----------------
1. Setup
    Install spark
    Install hadoop
    Install docker
    Download MinIO docker image
    Run docker-compose.yaml file in project root
    Start hadoop
    Start MinIO
    Copy appropriate jar files in spark jar folders to be able to connect to `hdfs`

2. Prepare data
   You should put users and tweets json files in MinIO. Ido this through `hdfs` and python code:
    .. code-block:: python

        hdfs dfs -put /home/adanic/users*.json /user/my_user/
        hdfs dfs -put /home/adanic/tweets*.json /user/my_user/
        df = sc.read.json("hdfs://localhost:9000/user/my_user/*.json")
   Create a path in minio and write json files into it:

 .. code-block:: python

        df.write.json("s3a://data/raw/tweets")
        df.write.json("s3a://data/raw/users")

3. ETL
    Run two etl files separately

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
