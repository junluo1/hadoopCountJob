package videoInfoJobTop10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * get the fifth field of the Text, which is the length of each instance and write out to Context.
 */
public class videoInfoTop10Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString();
        String [] fields = line.split("\t");
        String uid  = fields[0];
        long length = Long.parseLong(fields[4]);

        Text k2 = new Text();
        k2.set(uid);
        LongWritable v2 = new LongWritable();
        v2.set(length);

        context.write(k2, v2);
    }
}
