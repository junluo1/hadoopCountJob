package videoInfoJobTop10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class videoInfoTop10Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    HashMap <String, Long> map = new HashMap<>();

    /**
     * add up the video length of each uid and put into a hash map
     * @param k2
     * @param v2s
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
        long lengthSum = 0;
        for (LongWritable v2: v2s) {
            lengthSum += v2.get();
        }
        map.put(k2.toString(), lengthSum);
    }

    /**
     * initialize resource, execute only once
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    /**
     * close resources, execute only once
     * get the top 10 video length and correspondence uid, plus current date
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        //get date from configuration parameter
        String dt = conf.get("dt");

        Map<String, Long> sortedMap = MapUtils.sortValue(map);
        Set<Map.Entry<String, Long>> entries = sortedMap.entrySet();
        Iterator<Map.Entry<String, Long>> it = entries.iterator();
        int count = 1;
        while(count <= 10 && it.hasNext()){
            Map.Entry<String, Long> entry = it.next();
            String key = entry.getKey();
            Long value = entry.getValue();
            Text k3 = new Text();
            k3.set(dt+"\t"+key);
            LongWritable v3 = new LongWritable();
            v3.set(value);
            context.write(k3, v3);
            count++;
        }
    }
}
