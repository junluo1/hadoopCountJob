package videoInfo;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class videoInfoWritable implements Writable {

    private Long gold;
    private Long watchnumpv;
    private Long follower;
    private Long length;

    public void set(Long gold, Long watchnumpv, Long follower, Long length){
        this.gold = gold;
        this.watchnumpv = watchnumpv;
        this.follower = follower;
        this.length = length;
    }

    public Long getGold() {
        return gold;
    }

    public Long getWatchnumpv() {
        return watchnumpv;
    }

    public Long getFollower() {
        return follower;
    }

    public Long getLength() {
        return length;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.gold = dataInput.readLong();
        this.watchnumpv = dataInput.readLong();
        this.follower = dataInput.readLong();
        this.length = dataInput.readLong();

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(gold);
        dataOutput.writeLong(watchnumpv);
        dataOutput.writeLong(follower);
        dataOutput.writeLong(length);
    }

    @Override
    public String toString() {
        return gold +"\t" + watchnumpv + "\t"+ follower +"\t"+ length;
    }
}
