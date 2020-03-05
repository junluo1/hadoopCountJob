package videoInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class videoInfoReduce extends Reducer<Text, videoInfoWritable, Text, videoInfoWritable> {

    @Override
    protected void reduce(Text k2, Iterable<videoInfoWritable> v2s, Context context) throws IOException, InterruptedException {

        long goldSum = 0;
        long watchnumpvSum = 0;
        long followerSum = 0;
        long lengthSum = 0;

        for (videoInfoWritable v2: v2s) {
            goldSum += v2.getGold();
            watchnumpvSum += v2.getWatchnumpv();
            followerSum += v2.getFollower();
            lengthSum += v2.getLength();
        }

        Text k3 = k2;
        videoInfoWritable v3 = new videoInfoWritable();
        v3.set(goldSum, watchnumpvSum, followerSum, lengthSum);

        context.write(k3, v3);


    }
}
