package dataClean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * data clean class:
 * 1.filter Json doc, get the needed fields: uid, gold, watchnumpv, follower, length
 * 2.detect abnormal data in these fields
 * gold, watchnumpv, follower, length should not be negative or absent, if negative or absent, discard
 *
 * analysis:
 * 1.use fastjson to get the required fields
 * 2. keep normal data
 * 3.no Reduce stage needed, only need Map to filter data
 * k1,v1 <LongWritable, text>
 * k2,v2 <text, text> k2: uid v1: gold, watchnumpv, follower, length(separated by \t)
 */

public class dataCleanJob extends Mapper<LongWritable, Text, Text, LongWritable> {

    public static void main(String[] args) {
        try {
            //need input path and output path
            if(args.length!= 2){
                System.exit(100);
            }
            //the configuration parameters of job
            Configuration conf = new Configuration();
            //create a job
            Job job = Job.getInstance(conf);

            //declare wordCount class when executing in cluster
            job.setJarByClass(dataCleanJob.class);

            //set the input path(file or path
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            //set the output path(the path must be a nonexistent path)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));


            job.setMapperClass(dataCleanMap.class);
            //set the class of k2 and v2
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //reduce job 0
            job.setNumReduceTasks(0);

            //submit job
            job.waitForCompletion(true);

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
