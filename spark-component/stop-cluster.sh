#!/bin/bash

#stop livy
/home/ec2-user/livy/bin/livy-server stop

# Zeppelin stop
/home/ec2-user/zeppelin/bin/zeppelin-daemon.sh stop

# Spark history server Stop

/home/ec2-user/spark/sbin/stop-history-server.sh

# Spark stop
/home/ec2-user/spark/sbin/stop-all.sh

# Jobhistory stop
/home/ec2-user/hadoop/bin/mapred --daemon stop historyserver

# Hadoop stop
/home/ec2-user/hadoop/sbin/stop-all.sh
