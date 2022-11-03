#!/bin/bash

# Hadoop Run
/home/ec2-user/hadoop/sbin/start-all.sh

# Jobhistoryserver Run
/home/ec2-user/hadoop/bin/mapred --daemon start historyserver

# Spark Run
/home/ec2-user/spark/sbin/start-all.sh

# Spark history server Run
/home/ec2-user/spark/sbin/start-history-server.sh

# Zeppelin Run
/home/ec2-user/zeppelin/bin/zeppelin-daemon.sh start

# livy server run
/home/ec2-user/livy/bin/livy-server start
