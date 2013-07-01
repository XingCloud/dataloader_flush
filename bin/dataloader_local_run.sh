#!/bin/sh

basedir=`dirname $0`
cd $basedir/..
basedir=`pwd`

echo "dataloader home: $basedir"

jars=./conf
for jar in `find ./lib ./dist -name *.jar`
do
    jars=$jars:$jar
done

echo $jars

memarg="-server -Xms2G -Xmx2G"
gcarg="-XX:SurvivorRatio=4 -XX:+UseConcMarkSweepGC -XX:NewSize=64m -XX:+UseAdaptiveSizePolicy -XX:-ExplicitGCInvokesConcurrent -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=2"
verboses="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintGCApplicationStoppedTime"
main=com.xingcloud.dataloader.server.DataLoaderWatcher

fileencoding="-Dfile.encoding=UTF-8"

nohup java $fileencoding $memarg $gcarg $verboses -classpath $jars $main >/dev/null 2>&1 &

