
#!/bin/bash
export HDFS_NAMENODE_USER="hduser"
export HDFS_DATANODE_USER="hduser"
export HDFS_SECONDARYNAMENODE_USER="hduser"
export YARN_RESOURCEMANAGER_USER="hduser"
export YARN_NODEMANAGER_USER="hduser"
export HADOOP_HOME=/home/hduser/hdfs
export SPARK_HOME=/home/hduser/spark
export HADOOP_CONF_DIR=/home/hduser/hdfs/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export PATH=$PATH:$SPARK_HOME/bin
export PATH=$PATH:$SPARK_HOME/sbin
export HADOOP_MAPRED_HOME=${HADOOP_HOME}
export HADOOP_COMMON_HOME=${HADOOP_HOME}
export HADOOP_HDFS_HOME=${HADOOP_HOME}
export YARN_HOME=${HADOOP_HOME}
export HIVE_HOME=/home/hduser/hive
export HIVE_CONF_DIR=/home/hduser/hive/conf
export PATH=$PATH:$HIVE_HOME/bin
export OOZIE_HOME=/home/hduser/ozzie
export PATH=$PATH:$OOZIE_HOME/bin
export ZOOKEEPER_HOME=/home/hduser/zookeeper
export PATH=$PATH:$ZOOKEEPER_HOME/bin
export HBASE_HOME=/home/hduser/hbase
export PATH=$PATH:$HBASE_HOME/bin
export TEZ_JARS=/home/hduser/tez
export TEZ_CONF_DIR=$HADOOP_CONF_DIR
export HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*:${HADOOP_CLASSPATH}:${JAVA_JDBC_LIBS}:${MAPREDUCE_LIBS}:/home/hduser/hbase/lib/client-facing-thirdparty/*:/home/hduser/hbase/lib/*:/home/hduser/hdfs/share/hadoop/tools/lib/*
export SPARK_CLASSPATH=$SPARK_CLASSPATH:$HADOOP_HOME/share/hadoop/tools/lib/*
export SQOOP_HOME=/home/hduser/sqoop
export PATH=$SQOOP_HOME/bin:$PATH
export ACCUMULO_HOME=/home/hduser/accumulo
export PATH=$ACCUMULO_HOME/bin:$PATH
export HCAT_HOME=${HIVE_HOME}/hcatalog/
export PATH=$HCAT_HOME/bin:$PATH
export CLASSPATH=$CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*
export TO_HDFS=/home/hduser/spark/apps/to_hdfs
export UTILS=/home/hduser/spark/apps/utils
alias TRANSFER='cd /home/hduser/transfer/'
f=`date -d'1 day ago' +'%Y%m%d'`
/home/hduser/hive/bin/hive -v  --hiveconf par=$f  -f /home/hduser/hive/scripts/create_table_alan_flow_session_chats.hql
echo "Finalizando create table alan_flow_session_chats"

