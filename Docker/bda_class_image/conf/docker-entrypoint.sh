#!/bin/bash

# Start HDFS
/etc/init.d/ssh start
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh

# Sqoop hack
# Disgusting!  To run sqoop with --bindir /tmp and Hadoop will find the generated jar.
export HADOOP_CLASSPATH=`(hadoop classpath)`:/tmp

# MySQL
/etc/init.d/mysql start 

# Hive metastore
echo 'create database hive;' | mysql --user root
echo 'create database world;' | mysql --user root

mysql world --user root < /tmp/world.sql
echo "create user training identified by 'training';" | mysql --user root
echo "grant all privileges on *.* to training;" | mysql --user root

# Hive
hdfs dfs -mkdir /tmp
hdfs dfs -mkdir -p /user/hive/warehouse

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hive/lib/*

schematool -initSchema -dbType mysql

exec "$@"
