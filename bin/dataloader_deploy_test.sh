#!/bin/bash

#working dir
day=`date -d "${date}" +%d`
year=`date -d "${date}" +%Y`
month=`date -d "${date}" +%m`

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar

jar="dataloader_flush_fix.jar";
testjar="dataloader_flush_fix_test.jar";

pwd=$(cd "$(dirname "$0")"; pwd)
nowDir=`dirname $pwd`
dist=${nowDir}/dist
deployBin=${nowDir}/bin/deployBin





#log deploy info
if [ ! -d ${logDir} ]
then
	mkdir -p ${logDir}
fi

chmod 777 -R ${logDir}
cd ${nowDir}
svn up src
svn up lib
svnRevison=`svn info ${nowDir} |grep Revision|awk '{print $2;}'`
svnUrl=`svn info ${nowDir} |grep URL|awk '{print $2;}'`
echo deploy the URL:${svnUrl} Revsion:${svnRevison} at ${time} >> ${logDir}/deploy.log


#ant
if [ ! -d $dist ]
then
	mkdir -p $dist
fi
cd ${nowDir}
ant 
#***************
# test jar




#***************
#copy the jar 

hostliststr="192.168.1.141,192.168.1.142,192.168.1.143,192.168.1.144,192.168.1.145,192.168.1.147,192.168.1.148,192.168.1.150,192.168.1.151,192.168.1.152,192.168.1.134"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
	scp  ${dist}/${jar} ${node}:${runJar}/${testjar}
done




