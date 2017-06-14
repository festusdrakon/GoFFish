package in.dream_lab.goffish.sample;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Hullas on 18-05-2017.
 */
public class PairLongWritable implements Writable {
    public long id1;
    public long id2;
    public long id3;

    public PairLongWritable(long id1, long id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    public PairLongWritable(long id1, long id2, long id3) {
        this.id1 = id1;
        this.id2 = id2;
        this.id3 = id3;
    }

    public PairLongWritable() {
    }

    public long getId1() {
        return id1;
    }

    public long getId2() {
        return id2;
    }

    public long getId3() {
        return id3;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id1);
        dataOutput.writeLong(id2);
        dataOutput.writeLong(id3);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id1 = dataInput.readLong();
        id2 = dataInput.readLong();
        id3 = dataInput.readLong();
    }
}