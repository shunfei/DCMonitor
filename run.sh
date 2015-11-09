#!/bin/sh
nohup java \
-Xmx1g \
-server \
-XX:+PrintGCDetails \
-XX:+PrintGCTimeStamps \
-classpath config:target/dependencies/*:target/dcmonitor-0.1.4.jar \
com.sf.monitor.DCMonitor > /data/logs/dcmonitor/dcmonitor.run.out 2>&1 &
