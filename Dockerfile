FROM ubuntu:latest

WORKDIR /root

# install requisites
RUN apt-get update && apt-get install -y openssh-server openjdk-8-jdk ssh wget curl vim python3 && \
    rm -rf /var/lib/apt/lists/*
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py && \
    rm get-pip.py && \
    python3 -m pip install --upgrade pip setuptools

# Install Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.3.5.tar.gz && \
    tar -xzf hadoop-3.3.5.tar.gz && \
    mv hadoop-3.3.5 /usr/local/hadoop && \
    rm hadoop-3.3.5.tar.gz

# Install Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.0-bin-hadoop3.tgz && \
    mv spark-3.4.0-bin-hadoop3 /usr/local/spark && \
    rm spark-3.4.0-bin-hadoop3.tgz

# Install pyspark
RUN pip install pyspark 

# Install Kafka
RUN wget https://dlcdn.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz && \
    tar -xzf kafka_2.13-3.4.0.tgz && \
    mv kafka_2.13-3.4.0 /usr/local/kafka && \
    rm kafka_2.13-3.4.0.tgz

# Install HBase
RUN wget https://dlcdn.apache.org/hbase/2.4.17/hbase-2.4.17-bin.tar.gz && \ 
    tar -xzf hbase-2.4.17-bin.tar.gz && \
    mv hbase-2.4.17 /usr/local/hbase && \
    rm hbase-2.4.17-bin.tar.gz

# Install Zookeeper
#RUN wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz && \
#    tar -xzf apache-zookeeper-3.7.1-bin.tar.gz && \
#    mv apache-zookeeper-3.7.1-bin /usr/local/zookeeper && \
#    rm apache-zookeeper-3.7.1-bin.tar.gz

# # copy the test files
# RUN wget https://mohetn-my.sharepoint.com/:t:/g/personal/lilia_sfaxi_insat_u-carthage_tn/EWdosZTuyDtEiqcjpqbY_loBlfQbIQWp8Zq7PPKSAE1sjQ?e=O3TNLR && \ 
#     wget  https://mohetn-my.sharepoint.com/:t:/g/personal/lilia_sfaxi_insat_u-carthage_tn/EexZfjSnlShAqDig-0efjbkBJRiHqN0POQt0t4fvXhb7Dw?e=af6lAZ
# copy files
# RUN mv /tmp/files/* ~/ 

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
ENV HADOOP_HOME=/usr/local/hadoop 
ENV SPARK_HOME=/usr/local/spark
ENV KAFKA_HOME=/usr/local/kafka
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
ENV HBASE_HOME=/usr/local/hbase
#ENV ZOOKEEPER_HOME=/usr/local/zookeeper
ENV CLASSPATH=$CLASSPATH:/usr/local/hbase/lib/*
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/usr/local/kafka/bin:/usr/local/hbase/bin 

# ssh without key
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

RUN mkdir -p ~/hdfs/namenode && \
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs

COPY config/* /tmp/

COPY files /root/

RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop-env.sh /usr/local/hadoop/etc/hadoop/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml && \
    mv /tmp/workers $HADOOP_HOME/etc/hadoop/workers && \
    mv /tmp/start-kafka-zookeeper.sh ~/start-kafka-zookeeper.sh && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh && \
    mv /tmp/run-wordcount.sh ~/run-wordcount.sh && \
    mv /tmp/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf && \
    mv /tmp/hbase-env.sh $HBASE_HOME/conf/hbase-env.sh && \
    mv /tmp/hbase-site.xml $HBASE_HOME/conf/hbase-site.xml
#    cp /usr/local/zookeeper/conf/sample_zoo.conf /usr/local/zookeeper/conf/zoo.conf

RUN chmod +x ~/start-hadoop.sh && \
    chmod +x ~/start-kafka-zookeeper.sh && \
    chmod +x ~/run-wordcount.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh 

# format namenode
RUN /usr/local/hadoop/bin/hdfs namenode -format

CMD [ "sh", "-c", "service ssh start; bash"]

