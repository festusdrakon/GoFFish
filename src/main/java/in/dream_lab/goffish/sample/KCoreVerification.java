package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IMessage;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Created by Hullas on 23-05-2017.
 */
public class KCoreVerification extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, PairLongWritable, LongWritable, LongWritable, LongWritable>{
    @Override
    public void compute(Iterable<IMessage<LongWritable, PairLongWritable>> iterable) throws IOException {

    }
}
