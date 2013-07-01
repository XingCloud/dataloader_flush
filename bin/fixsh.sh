hostliststr="10.32.42.4,10.32.42.31,10.32.42.19,10.32.42.17"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`



for node in ${host} 
do
ssh ${node} /usr/lib/hadoop/bin/hadoop jar /home/hadoop/xa//runJar/dataloader.jar com.xingcloud.dataloader.DataLoader -d 20120418 -si 0 -ei 176 >/dev/null 2>&1  & 


done
