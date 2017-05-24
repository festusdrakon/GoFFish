package in.dream_lab.goffish.sample;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Hullas on 18-05-2017.
 */
public class PairLongWritable implements Writable {
    public LongWritable id1;
    public LongWritable id2;

    public PairLongWritable(LongWritable id1, LongWritable id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    public PairLongWritable() {
    }

    public LongWritable getId1() {
        return id1;
    }

    public LongWritable getId2() {
        return id2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id1.get());
        dataOutput.writeLong(id2.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id1 = new LongWritable(dataInput.readLong());
        id2 = new LongWritable(dataInput.readLong());
    }
}