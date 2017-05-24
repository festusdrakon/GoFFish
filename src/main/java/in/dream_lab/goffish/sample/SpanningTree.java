package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.*;

/**
 * Created by Hullas on 17-05-2017.
 */
public class SpanningTree extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, PairLongWritable, LongWritable, LongWritable, LongWritable> {

    boolean subgraphVisited = false;
    Map<LongWritable, PairLongWritable> messagePair = new HashMap<>();
    Map<LongWritable, Boolean> visited = new HashMap<>();
    List<IVertex<LongWritable, LongWritable, LongWritable, LongWritable>> queue = new ArrayList<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, PairLongWritable>> messages) throws IOException {
        if(getSuperstep()==0){
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                visited.put(vertex.getVertexId(), false);
            }
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> initVertex = getSubgraph().getLocalVertices().iterator().next();
            visited.put(initVertex.getVertexId(), true);
            queue.add(initVertex);
            BFS(queue.get(0));

            if(getSubgraph().getSubgraphId().compareTo(new LongWritable(0))==0){
                subgraphVisited = true;
                sendMessage();
            }
        }
        else if(messages.iterator().hasNext() && !subgraphVisited){
            subgraphVisited = true;
            IMessage<LongWritable, PairLongWritable> message = messages.iterator().next();
            System.out.println(message.getMessage().getId1() + " - " + message.getMessage().getId2());
            sendMessage();
        }
        voteToHalt();
    }


    public void BFS(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> source){
        for(IEdge<LongWritable, LongWritable, LongWritable> edge : source.getOutEdges()){
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbour = getSubgraph().getVertexById(edge.getSinkVertexId());
            if(!neighbour.isRemote()){
                if(!visited.get(neighbour.getVertexId())){
                    System.out.println(source.getVertexId() + " - " + neighbour.getVertexId());
                    visited.put(neighbour.getVertexId(), true);
                    queue.add(neighbour);
                }
            }
            else{
                messagePair.put(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
                        neighbour).getSubgraphId(),
                        new PairLongWritable(source.getVertexId(), neighbour.getVertexId()));
            }
            queue.remove(source);
            if(queue.size()>0)
                BFS(queue.get(0));
        }
    }

    public void sendMessage(){
       Iterator it = messagePair.entrySet().iterator();
       while (it.hasNext()) {
            Map.Entry<LongWritable, PairLongWritable> pair = (Map.Entry) it.next();
            sendMessage(pair.getKey(), pair.getValue());
       }
    }
}
