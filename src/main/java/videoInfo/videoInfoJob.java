package videoInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class videoInfoJob {

    /**
     * 1.use uid as key, reduce the data, get accumulated gold, watchnumpv, follower, length number for each unique uid
     * 2.need to define a customized Writable to combine the four fields as a object
     * 3.<k2, v2> <Text, customized Writable>
     *
     */

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
            job.setJarByClass(videoInfoJob.class);

            //set the input path(file or path
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            //set the output path(the path must be a nonexistent path)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));


            /**
             * combine map and reduce
             */
            job.setMapperClass(videoInfoMap.class);
            //set the class of k2 and v2
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(videoInfoWritable.class);

            job.setReducerClass(videoInfoReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(videoInfoJob.class);

            //submit job
            job.waitForCompletion(true);

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
