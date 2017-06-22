package in.dream_lab.goffish.sample;

import com.sun.org.apache.xpath.internal.operations.Mult;
import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Hullas on 21-06-2017.
 */
public class Bidirected extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, MultipleLongWritable, LongWritable, LongWritable, LongWritable> {

    Map<Long, List<Long>> pair = new HashMap<>();
    @Override
    public void compute(Iterable<IMessage<LongWritable, MultipleLongWritable>> iMessages) throws IOException {
        if(getSuperstep()==0){
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                for(IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()){
                    if(!getSubgraph().getVertexById(edge.getSinkVertexId()).isRemote()){
                        boolean check=false;
                        if(pair.get(edge.getSinkVertexId().get())!=null){
                            if(pair.get(edge.getSinkVertexId().get()).contains(vertex.getVertexId().get())){
                                check=true;
                            }
                        }
                        else {
                            for (IEdge<LongWritable, LongWritable, LongWritable> edge1 : getSubgraph().getVertexById(edge.getSinkVertexId()).getOutEdges()) {
                                if(edge1.getSinkVertexId().get()==vertex.getVertexId().get()){
                                    check=true;
                                    if(pair.get(vertex.getVertexId().get())==null)
                                        pair.put(vertex.getVertexId().get(), new ArrayList<Long>());
                                    pair.get(vertex.getVertexId().get()).add(edge.getSinkVertexId().get());
                                    break;
                                }
                            }
                        }
                        if(!check){
                            System.out.println("false local");
                            voteToHalt();
                        }
                    }
                    else{
                        if(pair.get(vertex.getVertexId().get())==null)
                            pair.put(vertex.getVertexId().get(), new ArrayList<Long>());
                        pair.get(vertex.getVertexId().get()).add(edge.getSinkVertexId().get());
                        sendMessage( ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) getSubgraph().getVertexById(edge.getSinkVertexId())).getSubgraphId(),
                                new MultipleLongWritable(vertex.getVertexId().get(), edge.getSinkVertexId().get()));
                    }
                }
            }
        }
        else if(getSuperstep()==1){
            for(IMessage<LongWritable, MultipleLongWritable> message : iMessages){
                if(!pair.get(message.getMessage().getId2()).contains(message.getMessage().getId1())){
                    System.out.println("false remote");
                    voteToHalt();
                }
            }
        }
        else{
            System.out.println("superstep error " + getSuperstep());
        }
        voteToHalt();
    }
}
