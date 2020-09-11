===========
etl-pipeline
===========


Reading JSON files from Minio, process them using Spark, write them back to Minio as CSV files


Getting Started
--------
Install spark
Install hadoop
Install docker
Download MinIO docker image
Create docker compose `yaml` file

Start hadoop
Start MinIO

hdfs dfs -put /home/adanic/users*.json /user/my_user/
hdfs dfs -put /home/adanic/tweets*.json /user/my_user/

```
df = sc.read.json("hdfs://localhost:9000/user/my_user/*.json")
```

Create a path in minio and write json files into it:

```
df.write.json("s3a://data/raw/tweets")
df.write.json("s3a://data/raw/users")
```

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
