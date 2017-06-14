package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.*;

/**
 * Created by Hullas on 08-06-2017.
 */
public class SpanningForest extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, PairLongWritable, LongWritable, LongWritable, LongWritable>
        implements ISubgraphWrapup{

    long cid;
    PairLongWritable edge=null;
    Map<LongWritable, PairLongWritable> messagePair = new HashMap<>();
    Map<LongWritable, Boolean> visited = new HashMap<>();
    List<IVertex<LongWritable, LongWritable, LongWritable, LongWritable>> queue = new ArrayList<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, PairLongWritable>> messages) throws IOException {
        if(getSuperstep()==0){
            cid=getSubgraph().getSubgraphId().get();
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                visited.put(vertex.getVertexId(), false);
            }
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> initVertex = getSubgraph().getLocalVertices().iterator().next();
            visited.put(initVertex.getVertexId(), true);
            queue.add(initVertex);
            BFS(queue.get(0));

            sendMessage();
        }
        else{
            boolean changed = false;
            for(IMessage<LongWritable, PairLongWritable> message : messages){
                if(message.getMessage().getId3()<cid){
                    cid=message.getMessage().getId3();
                    edge = message.getMessage();
                    changed = true;
                }
            }
            if(changed){
                sendMessage();
            }
        }
        voteToHalt();
    }

    public void BFS(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> source){
        for(IEdge<LongWritable, LongWritable, LongWritable> edge : source.getOutEdges()) {
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbour = getSubgraph().getVertexById(edge.getSinkVertexId());
            if (!neighbour.isRemote()) {
                if (!visited.get(neighbour.getVertexId())) {
                    System.out.println(source.getVertexId() + " - " + neighbour.getVertexId());
                    visited.put(neighbour.getVertexId(), true);
                    queue.add(neighbour);
                }
            }
            else {
                    messagePair.put(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
                                    neighbour).getSubgraphId(),
                            new PairLongWritable(source.getVertexId().get(), neighbour.getVertexId().get()));
            }
        }
        queue.remove(source);
        if(queue.size()>0){
            BFS(queue.get(0));
        }
    }

    public void sendMessage(){
        Iterator it = messagePair.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<LongWritable, PairLongWritable> pair = (Map.Entry) it.next();
            sendMessage(pair.getKey(), new PairLongWritable(pair.getValue().getId1(), pair.getValue().getId2(), cid));
        }
    }

    @Override
    public void wrapup() throws IOException {
        if(cid!=getSubgraph().getSubgraphId().get())
            System.out.println(edge.getId1() + " - " + edge.getId2());
    }
}

