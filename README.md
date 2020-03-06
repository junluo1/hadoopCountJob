# HadoopCountJob
This project implements a count job using hadoop mapReduce.<br>
1. Clean data cleans the raw json data by filtering abnormal data, rewrote the map method for Mapper class<br>
2. Video count gets the required fields of each instance and aggregates the instances for each unique user id<br>
3. Video count job top 10 caluculates the accumulative length of video time for each unique user id, sort the data and get the top10 video length and correspondent user id, return data with an additional date field for the record. Achieve sorting in the override cleanup method.
