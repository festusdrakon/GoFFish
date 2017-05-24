package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.*;

/**
 * Created by Hullas on 22-05-2017.
 */
public class KCore extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, PairLongWritable, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup{

    private Map<LongWritable, Integer> core = new HashMap<>();
    private List<vertexDegree> verticesSorted = new LinkedList<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, PairLongWritable>> iMessages) throws IOException {
        if(getSuperstep()==0){
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                Collection<IEdge<LongWritable, LongWritable, LongWritable>> edges = (Collection<IEdge<LongWritable, LongWritable, LongWritable>>) vertex.getOutEdges();
                verticesSorted.add(new vertexDegree(vertex.getVertexId(),
                        edges.size()));
                core.put(vertex.getVertexId(), edges.size());
            }
            Collections.sort(verticesSorted, new CompareVertex());

            for(int i=0; i<verticesSorted.size(); i++){
                vertexDegree vertex = verticesSorted.get(0);
                for(IEdge<LongWritable, LongWritable, LongWritable> edge : getSubgraph().getVertexById(vertex.getVertexId()).getOutEdges()){
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbour = getSubgraph().getVertexById(edge.getSinkVertexId());
                    if(!neighbour.isRemote()){
                        if(core.get(neighbour.getVertexId()) > vertex.getDegree()){
                            core.put(neighbour.getVertexId(), core.get(neighbour.getVertexId()) - 1);
                            verticesSorted.get(find(neighbour.getVertexId())).setDegree(core.get(neighbour.getVertexId()));
                        }
                    }
                    else{
                        sendMessage(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) neighbour).getSubgraphId(),
                                new PairLongWritable(neighbour.getVertexId(), new LongWritable(vertex.getDegree())));
                    }
                }
                verticesSorted.remove(0);
                Collections.sort(verticesSorted, new CompareVertex());
            }
        }

        else{
            if(iMessages.iterator().hasNext()) {
                Map<LongWritable, Boolean> update = new HashMap<>();

                for (IMessage<LongWritable, PairLongWritable> message : iMessages) {
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex = getSubgraph().getVertexById(message.getMessage().getId1());
                    if (core.get(vertex.getVertexId()) > message.getMessage().getId2().get()) {
                        core.put(vertex.getVertexId(), core.get(vertex.getVertexId()) - 1);
                        update.put(vertex.getVertexId(), true);
                    }
                }

                for (Map.Entry<LongWritable, Boolean> vertexId : update.entrySet())
                    updateCore(vertexId.getKey());

                update = null;
             }
        }
        voteToHalt();
    }

    private void updateCore(LongWritable vertexId) {
       IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex = getSubgraph().getVertexById(vertexId);
       for(IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
           IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbour = getSubgraph().getVertexById(edge.getSinkVertexId());
           if(!neighbour.isRemote()){
               if(core.get(edge.getSinkVertexId()) > core.get(vertexId)){
                   core.put(edge.getSinkVertexId(), core.get(edge.getSinkVertexId()) - 1);
                   updateCore(edge.getSinkVertexId());
               }
           }
           else{
               sendMessage(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) neighbour).getSubgraphId(),
                       new PairLongWritable(neighbour.getVertexId(), new LongWritable(core.get(vertexId))));
           }
       }
    }

    @Override
    public void wrapup() throws IOException {
//      Map<Integer, List<LongWritable>> coreNumber = new HashMap<>();

        for (Map.Entry<LongWritable, Integer> entry : core.entrySet()) {
           /*if (coreNumber.get(entry.getValue()) == null)
                coreNumber.put(entry.getValue(), new LinkedList<LongWritable>());
            coreNumber.get(entry.getValue()).add(entry.getKey());*/
            System.out.println(entry.getKey() + " core: " + entry.getValue());
        }

        /*List<Integer> keys = new ArrayList<>(coreNumber.keySet());
        Collections.sort(keys, Collections.reverseOrder());

        for (Integer element : keys) {
            System.out.println(element + " := " + coreNumber.get(element));
        }*/
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