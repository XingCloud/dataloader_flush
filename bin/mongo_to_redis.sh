#!/bin/sh
base_dir=`dirname $0`/..

memarg="-server -Xms1g -Xmx1g -Xss128K"
gcarg="-XX:SurvivorRatio=16 -XX:+UseConcMarkSweepGC -XX:NewSize=512M -XX:MaxNewSize=512M -XX:+UseAdaptiveSizePolicy -XX:-ExplicitGCInvokesConcurrent -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=2"

verboses="-XX:+HeapDumpOnOutOfMemoryError"

main=com.xingcloud.dataloader.tools.MongoOfflineResult2Redis
fileencoding="-Dfile.encoding=UTF-8"
classpath=$base_dir/dist/dataloader.jar

nohup java $fileencoding $memarg $gcarg $verboses -classpath $classpath $main > mongotoredis.log 2>&1  &



