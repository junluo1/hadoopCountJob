package videoInfo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class videoInfoMap extends Mapper <LongWritable, Text, Text, videoInfoWritable>{

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString();
        //split fields
        String []fields = line.split("\t");
        String id  = fields[0];
        Long gold = Long.parseLong(fields[1]);
        Long matchnumpv = Long.parseLong(fields[2]);
        Long follower = Long.parseLong(fields[3]);
        Long length = Long.parseLong(fields[4]);
        //get k2
        Text k2 = new Text();
        k2.set(id);
        //get v2 as an videoInfoWritable object
        videoInfoWritable v2 = new videoInfoWritable();
        v2.set(gold, matchnumpv,follower,length);

        //write the context
        context.write(k2,v2);
    }
}
