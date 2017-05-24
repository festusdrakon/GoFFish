package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IVertex;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Created by Hullas on 19-05-2017.
 */
public class Directed extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {


    @Override
    public void compute(Iterable<IMessage<LongWritable, LongWritable>> iterable) throws IOException {
        if(getSuperstep()==0){
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                for(IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()){
                    if(!getSubgraph().getVertexById(edge.getSinkVertexId()).isRemote()){
                        boolean found = false;
                        for(IEdge<LongWritable, LongWritable, LongWritable> revEdge : getSubgraph().getVertexById(edge.getSinkVertexId()).getOutEdges()){
                            if(revEdge.getSinkVertexId().equals(vertex.getVertexId()))
                                found=true;
                        }
                        if(!found){
                            System.out.println("Directed");
                            voteToHalt();
                        }
                    }
                }
            }

        }
        System.out.println("Undirected");
        voteToHalt();
    }

}
