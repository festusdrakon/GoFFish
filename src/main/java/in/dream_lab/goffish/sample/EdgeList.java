package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IVertex;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Created by Hullas on 24-06-2017.
 */
public class EdgeList extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, MultipleLongWritable, LongWritable, LongWritable, LongWritable> {
    @Override
    public void compute(Iterable<IMessage<LongWritable, MultipleLongWritable>> iterable) throws IOException {
        for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
            for(IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges())
                System.out.println(vertex.getVertexId() + " " + edge.getSinkVertexId());
        }
        voteToHalt();
    }
}