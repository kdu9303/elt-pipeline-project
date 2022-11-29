#!/usr/bin/env bash

spark.master              yarn
spark.eventLog.enabled    true
spark.eventLog.dir                  hdfs://master:9000/spark-history
spark.history.fs.logDirectory       hdfs://master:9000/spark-history
spark.history.fs.cleaner.interval   7d
spark.history.fs.cleaner.maxAge     14d
spark.history.retainedApplications  10
spark.yarn.historyServer.address master:18081
spark.history.ui.port 				18081
