package dataClean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class dataCleanMap extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * filter required fields
     * determine abnormal data
     * @param k1
     * @param v1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //get every doc
        String line = v1.toString();
        //transform doc to Json object
        JSONObject jsonObj = JSON.parseObject(line);
        String id = jsonObj.getString("uid");
        int gold = jsonObj.getIntValue("gold");
        int watchnumpv = jsonObj.getIntValue("watchnumpv");
        int follower = jsonObj.getIntValue("follower");
        int length = jsonObj.getIntValue("length");

        //filter abnormal data
        if(!id.equals("null") && gold>=0 && watchnumpv>=0 && follower>=0 && length>=0){
            //set values
            Text k2 = new Text();
            k2.set(id);
            Text v2 = new Text();
            v2.set(gold+"\t"+watchnumpv+"\t"+follower+"\t"+length);
            context.write(k2,v2);
        }

    }
}
