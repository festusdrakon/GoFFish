/*
package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.*;


//  Created by Hullas on 15-06-2017.


public class KCore extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, SpanningForest.PairLongWritable, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup{

    private Map<LongWritable, Integer> core = new HashMap<>();
    private List<vertexDegree> verticesSorted = new LinkedList<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, SpanningForest.PairLongWritable>> iMessages) throws IOException {
        if(getSuperstep()==0){
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                Collection<IEdge<LongWritable, LongWritable, LongWritable>> edges = (Collection<IEdge<LongWritable, LongWritable, LongWritable>>) vertex.getOutEdges();
                verticesSorted.add(new vertexDegree(vertex.getVertexId(),
                        edges.size()));
                core.put(vertex.getVertexId(), edges.size());
            }
            Collections.sort(verticesSorted, new CompareVertex());
            if(verticesSorted.size()>0)
                sendMessage(new LongWritable(1), new SpanningForest.PairLongWritable(getSubgraph().getSubgraphId().get(), verticesSorted.get(0).getDegree()));
        }

        else if(getSuperstep()%3==1 && getSubgraph().getSubgraphId().get()==1){
            long min = iMessages.iterator().next().getMessage().getId2();

            for(IMessage<LongWritable, SpanningForest.PairLongWritable> message : iMessages){
                if(message.getMessage().getId2()<min)
                    min = message.getMessage().getId2();
            }

            for(IMessage<LongWritable, SpanningForest.PairLongWritable> message : iMessages){
                if(message.getMessage().getId2()==min){
                    sendMessage(new LongWritable(message.getMessage().getId1()), new SpanningForest.PairLongWritable(0));
                }
            }
            if(verticesSorted.size()==0)
                sendMessage(new LongWritable(1), new SpanningForest.PairLongWritable(-1));
            else if(min!=verticesSorted.get(0).getDegree())
                sendMessage(new LongWritable(1), new SpanningForest.PairLongWritable(-1));
        }
        else if(getSuperstep()%3==2){
            if(getSubgraph().getSubgraphId().get()==1){
                sendToAll(new SpanningForest.PairLongWritable(-1));
            }
            else if(iMessages.iterator().next().getMessage().getId1()==0) {
                int min = verticesSorted.get(0).getDegree();
                while (verticesSorted.size() > 0) {
                    if (verticesSorted.get(0).getDegree() == min) {
                        vertexDegree vertex = verticesSorted.get(0);
                        for (IEdge<LongWritable, LongWritable, LongWritable> edge : getSubgraph().getVertexById(vertex.getVertexId()).getOutEdges()) {
                            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbour = getSubgraph().getVertexById(edge.getSinkVertexId());
                            if (!neighbour.isRemote()) {
                                if (core.get(neighbour.getVertexId()) > vertex.getDegree()) {
                                    if (find(vertex.getVertexId()) != -1) {
                                        core.put(neighbour.getVertexId(), core.get(neighbour.getVertexId()) - 1);
                                        verticesSorted.get(find(neighbour.getVertexId())).setDegree(core.get(neighbour.getVertexId()));
                                    }
                                }
                            } else {
                                sendMessage(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) neighbour).getSubgraphId(),
                                        new SpanningForest.PairLongWritable(neighbour.getVertexId().get(), vertex.getDegree()));
                            }
                        }
                        verticesSorted.remove(0);
                        Collections.sort(verticesSorted, new CompareVertex());
                    } else
                        break;
                }
            }
        }
        else if(getSuperstep()%3==0){
            for(IMessage<LongWritable, SpanningForest.PairLongWritable> message : iMessages){
                if(message.getMessage().getId1()==-1)
                    continue;
                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex = getSubgraph().getVertexById(new LongWritable(message.getMessage().getId1()));
                if ( core.get(vertex.getVertexId()) > message.getMessage().getId2()) {
                    if(find(vertex.getVertexId())!=-1){
                        verticesSorted.get(find(vertex.getVertexId())).setDegree(core.get(vertex.getVertexId()) - 1);
                        core.put(vertex.getVertexId(), core.get(vertex.getVertexId()) - 1);
                    }
                }
            }
            Collections.sort(verticesSorted, new CompareVertex());
            if(verticesSorted.size()>0)
                sendMessage(new LongWritable(1), new SpanningForest.PairLongWritable(getSubgraph().getSubgraphId().get(), verticesSorted.get(0).getDegree()));
        }
        voteToHalt();
    }

    @Override
    public void wrapup() throws IOException {
        for (Map.Entry<LongWritable, Integer> entry : core.entrySet())
            System.out.println(entry.getKey() + " core: " + entry.getValue());
    }

    static class vertexDegree{
        LongWritable vertexId;
        int degree;

        LongWritable getVertexId() {
            return vertexId;
        }

        int getDegree() {
            return degree;
        }

        vertexDegree(LongWritable vertexId, int degree) {
            this.vertexId = vertexId;
            this.degree = degree;
        }

        void setDegree(int degree) {
            this.degree = degree;
        }
    }

    public static class CompareVertex implements Comparator<vertexDegree> {
        public int compare(vertexDegree o1, vertexDegree o2) {
            return o1.degree - o2.degree;
        }
    }

    public int find(LongWritable id){
        int index = 0;
        for(vertexDegree temp : verticesSorted){
            if(temp.getVertexId().equals(id)){
                return index;
            }
            index++;
        }
        return -1;
    }
}
*/
