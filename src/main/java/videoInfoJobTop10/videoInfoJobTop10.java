package videoInfoJobTop10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * collect the uid and length for top 10 length
 *
 * get the uid and length in map stage <k2, v2>is <Text, LongWritable>
 * In reduce stage, collect data into a map set
 * use clean up in reduce to sort the length
 * get top 10 and write to hdfs file
 *
 */
public class videoInfoJobTop10 extends Reducer<Text, LongWritable, Text, LongWritable> {

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

            //get date through input path
            String[] fields = args[0].split("/");
            String tmpdt = fields[fields.length - 1];
            String dt = DateUtils.transDataFormat(tmpdt);
            conf.set("dt", dt);

            //declare wordCount class when executing in cluster
            job.setJarByClass(videoInfoJobTop10.class);

            //set the input path(file or path
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            //set the output path(the path must be a nonexistent path)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            /**
             * combine map and reduce execution
             */
            job.setMapperClass(videoInfoTop10Map.class);
            //set the class of k2 and v2
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(videoInfoTop10Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //submit job
            job.waitForCompletion(true);

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
