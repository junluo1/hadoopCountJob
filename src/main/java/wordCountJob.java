import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * read a file from hdfs and count the occurrence of each word in the file
 * input file.txt: hello world hello hadoop
 * output:      hello 2
 *              world 1
 *              hadoop1
 */

public class wordCountJob {
    /**
     * create customized mapper class
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        /**
         * this function accepts (k1,v1) parameters and returns (k2,v2)
         * @param k1
         * @param v1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            // k1 represents the offset of the beginning of every line, v1 represents the contents of every line
            // split the words
            String[] words = v1.toString().split(" ");
            //interate the words and write into context as(k2,v2)
            for (String word: words) {
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                System.out.println("k2: "+word+" v2: 1");
                context.write(k2, v2);
            }
        }
    }

    /**
     * create customized Reducer class
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        /**
         * count the occurrences in v2List and returns (k3, v3)
         * @param k2
         * @param v2List
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2List, Context context) throws IOException, InterruptedException {
            //create a sum variable
            long sum = 0L;
            for (LongWritable v2: v2List) {
                sum += v2.get();
            }
            //combine text and sum as (k3,v3)
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            System.out.println("k3: "+k3.toString()+" v3: "+ sum);
            context.write(k3, v3);
        }
    }


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
            job.setJarByClass(wordCountJob.class);

            //set the input path(file or path
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            //set the output path(the path must be a nonexistent path)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));


            /**
             * combine map and reduce
             */
            job.setMapperClass(MyMapper.class);
            //set the class of k2 and v2
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //submit job
            job.waitForCompletion(true);

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
