package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by Hullas on 20-06-2017.
 */
public class KCore extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongIntWritable, LongWritable, LongWritable, LongWritable>
        implements ISubgraphWrapup{

    Map<Long, Integer> core = new HashMap<>();
    Map<Long, Set<Long>> neighbours = new HashMap<>();
    Set<Long> changed = new HashSet<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, LongIntWritable>> iMessages) throws IOException {
        if(getSuperstep()==0){
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                core.put(vertex.getVertexId().get(), ((Collection<IEdge<LongWritable, LongWritable, LongWritable>>) vertex.getOutEdges()).size());
                for(IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()){
                    if(getSubgraph().getVertexById(edge.getSinkVertexId()).isRemote()){
                        if(neighbours.get(vertex.getVertexId().get())==null)
                            neighbours.put(vertex.getVertexId().get(), new HashSet<Long>());
                        neighbours.get(vertex.getVertexId().get()).add(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
                                getSubgraph().getVertexById(edge.getSinkVertexId())).getSubgraphId().get() );
                    }
                }
            }
            changed.addAll(neighbours.keySet());
            sendMessages();
        }

        else{
            for(IMessage<LongWritable, LongIntWritable> message : iMessages){
                core.put(message.getMessage().getId1(), message.getMessage().getId2());
            }
            localEstimate();
            sendMessages();
        }

        voteToHalt();
    }

    private boolean computeCore(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v){
        int k = core.get(v.getVertexId().get());
        int count[] = new int[k];
        for(int i=0;i<k;i++)
            count[i]=0;
        for(IEdge<LongWritable, LongWritable, LongWritable> edge : v.getOutEdges()){
            int j = Math.min(k,core.get(edge.getSinkVertexId().get()));
            for(int i=0;i<j;i++)
                count[i]+=1;
        }
        for(int i=k-1;i>=0;i--){
            if(count[i]>=i+1){
                if(i==k-1)
                    return false;
                core.put(v.getVertexId().get(), i+1);
                if(neighbours.get(v.getVertexId().get())!=null)
                    changed.add(v.getVertexId().get());
                return true;
            }
        }
        return false;
    }

    private void localEstimate(){
        boolean repeat = true;
        while(repeat){
            repeat = false;
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                if(computeCore(vertex)){
                    repeat=true;
                    break;
                }
            }
        }
    }

    private void sendMessages(){
        for(Long changedVertex : changed){
            for(Long nbrSG : neighbours.get(changedVertex)){
                sendMessage(new LongWritable(nbrSG), new LongIntWritable(changedVertex, core.get(changedVertex)));
            }
        }
        changed.clear();
    }

    @Override
    public void wrapup() throws IOException {
        for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
            System.out.println("VertexId: " + vertex.getVertexId().get() + " core: " + core.get(vertex.getVertexId().get()));
        }
    }
}

class LongIntWritable implements Writable {

    public long id1;
    public int id2;

    public LongIntWritable(){}

    public LongIntWritable(long id1, int id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    public long getId1() {
        return id1;
    }

    public int getId2() {
        return id2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id1);
        dataOutput.writeInt(id2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id1 = dataInput.readLong();
        id2 = dataInput.readInt();
    }
}
