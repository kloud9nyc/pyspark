zip -ru9 packages.zip sparkConf read transform write schemas -x sparkConf/__pycache__/\* -x read/__pycache__/\* -x write/__pycache__/\* -x transform/__pycache__/\*  -x schemas/__pycache__/\*
    cp packages.zip ~/.
    cp jobs/job1.py ~/.
    cp configs/etl_config.json ~/.
    export ENV=local
    cd

    spark-submit --py-files packages.zip --driver-memory 2g --driver-java-options "-Dconfig_domain=hdfs -Dconfig_url=/Users/nithya/PycharmProjects/PyFlow/configs/etl_config.json" job1.py
    exit 0


#JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/
#
#SPARK_HOME=/Users/nithya/spark
#
#HADOOP_HOME=/Users/nithya/hadoop
#
#HADOOP_CONF_DIR=/Users/nithya/hadoop/etc/hadoop
#
#LC_ALL=en_US.utf-8
#LC_CTYPE=UTF-8
#LANG=en_US.utf-8
#
#PATH=/Library/Frameworks/Python.framework/Versions/3.6/bin:/Users/nithya/spark/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Library/Apple/usr/bin:/Users/nithya/hadoop/bin
#
#
#AWS_SECRET_ACCESS_KEY=XXXXXXX
#
#AWS_ACCESS_KEY_ID=XXXXXXXX
#
#PYTHONPATH=/Users/nithya/spark/python:/Users/nithya/spark/python/lib/py4j-0.10.7-src.zip:
