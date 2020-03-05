#!/bin/bash

# add this into crontab vi /etc/crontab, set a time(00:30) for daily execution
# 30 00 * * * root /bin/sh /data/soft/jobs/data_clean.sh >> /usr/hadoop/jobs/data_clean.log

#upload mvn sanpshot package before execution

# if user didn't input a date, set date to be yesterday as default
if [ "X$1" = "X" ]
then
  yes_time=`date +%y%m%d --date="1 days ago"`
else
  yes_time=$1
fi

cleanjob_input=hdfs://hadoop100:9000/data/videoinfo/${yes_time}
cleanjob_output=hdfs://hadoop100:9000/data/videoinfo_clean/${yes_time}

videoinfojob_input=${cleanjob_output}
videoinfojob_output=hdfs://hadoop100:9000/res/videoinfojob/${yes_time}

videoinfojobtop10_input=${cleanjob_output}
videoinfojobtop10_output=hdfs://hadoop100:9000/res/videoinfojobtop10/${yes_time}

#check if exist, not, create directry
jobs_home=/usr/hadoop/jobs

# delete output dir, in case need to rerun the script
hdfs dfs -rm -r ${cleanjob_output}
hdfs dfs -rm -r ${videoinfojob_output}
hdfs dfs -rm -r ${videoinfojobtop10_output}


# execute dataclean job
hadoop jar \
${jobs_home}/hadoopDemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
dataClean.DataCleanJob \
${cleanjob_input} \
${cleanjob_output}

# check if dataclean job succeed
hdfs dfs -ls ${cleanjob_output}/_SUCCESS
if [ "$?" = "0" ]
then
  echo "cleanjob execute success..."
  #execute videoinfo job1
  hadoop jar \
  ${jobs_home}/hadoopDemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
  videoinfo.videoInfoJob \
  ${videoinfojob_input} \
  ${videoinfojob_output}
  hdfs dfs -ls ${videoinfojob_output}/_SUCCESS
  if [ "$?" != "0" ]
    then
      echo "ViedoInfoJob execution fialed..."
      # send text to administrator
      # use while to execute for 3 times
      # use api/interface or script to send text
  fi

  #execute videoinfo job2
  hadoop jar \
  ${jobs_home}/hadoopDemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
  videoinfo.videoInfoJob \
  ${videoinfojobtop10_input} \
  ${videoinfojobtop10_output}
  hdfs dfs -ls ${videoinfojobtop10_output}/_SUCCESS
  if [ "$?" != "0" ]
    then
      echo "ViedoInfoJobTop10 execution fialed..."
      # send text to administrator
      # use while to execute for 3 times
      # use api/interface or script to send text
  fi


else
  echo"clean job execution failed...datetime is ${yes_time}"
  # send text to administrator
  # use while to execute for 3 times
  # use api/interface or script to send text
fi