FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

RUN apt-get -y update

RUN  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
	  python3 \ 
	  emacs \
      wget \
      vim \
	  nano \
      mysql-server \
      procps \
	  ssh \
	  openssh-client \
	  openssh-server

RUN rm -rf /var/lib/apt/lists/*

COPY conf/.emacs /.emacs

COPY world.sql /tmp/world.sql
COPY airline.sql /tmp/airline.sql
COPY textcorpora /data/textcorpora


######################
# Sqoop

RUN wget https://downloads.apache.org/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
#COPY sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz /sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz

RUN tar -xzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
RUN mv sqoop-1.4.7.bin__hadoop-2.6.0 /opt/sqoop
ENV SQOOP_HOME=/opt/sqoop
ENV PATH=$PATH:$SQOOP_HOME/bin

RUN cd /opt/sqoop/conf && \
	mv sqoop-env-template.sh sqoop-env.sh && \
	echo "export HADOOP_COMMON_HOME=/opt/hadoop-3.2.1" >> sqoop-env.sh && \
	echo "export HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" >> sqoop-env.sh

RUN rm sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz

# Need an older version of Apache Common Lang b/c to get StringUtils
COPY commons-lang-2.6.jar /opt/sqoop/lib

# Make the warnings stop....
ENV HBASE_HOME='.'
ENV ACCUMULO_HOME='.'
ENV ZOOKEEPER_HOME='.'

#######################
# MySQL

#RUN wget http://ftp.ntu.edu.tw/MySQL/Downloads/Connector-J/mysql-connector-java-8.0.23.tar.gz
#COPY mysql-connector-java-8.0.23.tar.gz /mysql-connector-java-8.0.23.tar.gz

#RUN tar -xvf mysql-connector-java-8.0.23.tar.gz
#RUN cp /mysql-connector-java-8.0.23/mysql-connector-java-8.0.23.jar /opt/sqoop/lib/
#RUN rm mysql-connector-java-8.0.23.tar.gz
#RUN rm -rf mysql-connector-java-8.0.23

COPY mysql-connector-java-8.0.23.jar opt/sqoop/lib/mysql-connector-java-8.0.23.jar

# Sample database
COPY world.sql /tmp/world.sql
COPY airline.sql /tmp/airline.sql


#######################
# Hadoop Configuration

COPY conf/core-site.xml /opt/hadoop-3.2.1/etc/hadoop/core-site.xml
COPY conf/hdfs-site.xml /opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 0600 ~/.ssh/authorized_keys

######################
# Hive
# https://phoenixnap.com/kb/install-hive-on-ubuntu

RUN wget https://downloads.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
#COPY apache-hive-3.1.2-bin.tar.gz /apache-hive-3.1.2-bin.tar.gz

RUN tar -xvf apache-hive-3.1.2-bin.tar.gz
RUN mv apache-hive-3.1.2-bin /opt/hive

ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin
ENV HIVE_CONF_DIR=/opt/hive/conf

ENV HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/hive/lib/*

RUN echo "export HADOOP_HOME=/opt/hadoop-3.2.1" >> $HIVE_HOME/bin/hive-config.sh
RUN rm $HIVE_HOME/lib/guava-19.0.jar && rm $HIVE_HOME/lib/log4j-slf4j-impl-2.10.0.jar
RUN cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-27.0-jre.jar $HIVE_HOME/lib/

COPY conf/hive-site.xml /opt/hive/conf

COPY conf/hive-exec-log4j2.properties /opt/hive/conf
COPY conf/llap-daemon-log4j2.properties /opt/hive/conf
COPY conf/hive-log4j2.properties /opt/hive/conf       
COPY conf/llap-cli-log4j2.properties /opt/hive/conf

#RUN wget http://ftp.ntu.edu.tw/MySQL/Downloads/Connector-J/mysql-connector-java-8.0.23.tar.gz
#RUN tar -xvf mysql-connector-java-8.0.23.tar.gz
#RUN cp /mysql-connector-java-8.0.23/mysql-connector-java-8.0.23.jar /opt/hive/conf/
#RUN rm apache-hive-3.1.2-bin.tar.gz

COPY mysql-connector-java-8.0.23.jar /opt/hive/lib/mysql-connector-java-8.0.23.jar


##############################

COPY conf/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod 755 /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
#ENTRYPOINT ["/bin/bash"]
