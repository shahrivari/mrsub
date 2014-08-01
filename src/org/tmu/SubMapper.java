package org.tmu;

import com.carrotsearch.hppc.LongLongOpenHashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.tmu.subdigger.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: saeed
 * Date: 5/5/13
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class SubMapper
        extends Mapper<Text, IntWritable, Text, LongWritable> {
    //public ImmutableGraph graph = null;
    public Graph graph = null;
    FreqMap labelMap = new FreqMap();
    LongLongOpenHashMap labelHPPCMap = new LongLongOpenHashMap(1024, 0.5f);
    public int flushTreshold = 512 * 1024;

    boolean beMemoryFriend = false;

    private void flushMaps(Context context, int treshold) throws IOException, InterruptedException {
        if (labelMap.size() > treshold || labelHPPCMap.size() > treshold) {
            for (Map.Entry<BoolArray, FreqMap.Count> entry : labelMap.map.entrySet())
                context.write(new Text(entry.getKey().getAsBigInt().toString(32)), new LongWritable(entry.getValue().get()));
            labelMap.clear();

            for (int i = 0; i < labelHPPCMap.keys.length; i++)
                if (labelHPPCMap.allocated[i])
                    context.write(new Text(Long.toString(labelHPPCMap.keys[i], 32)), new LongWritable(labelHPPCMap.values[i]));
            labelHPPCMap.clear();
            context.getCounter(MRSUBCounters.HashMap_Spills).increment(1);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (graph == null) {
            Path pt = new Path(context.getConfiguration().get("input_path"));
            FileSystem fs = FileSystem.get(context.getConfiguration());

            graph = HashGraph.readStructure(new InputStreamReader(fs.open(pt)));
            if (graph.vertexCount() < 15000)
                graph = MatGraph.readStructure(new InputStreamReader(fs.open(pt)));

            graph.printInfo();
            context.getCounter(MRSUBCounters.Graph_Loaded).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flushMaps(context, 0);
    }


    public void map(Text state_str, IntWritable value, Context context
    ) throws IOException, InterruptedException {
        int k = value.get();
        int v = Integer.parseInt(state_str.toString().split(" ")[0]);
        int w = Integer.parseInt(state_str.toString().split(" ")[1]);
        SMPState state = SMPState.makeState(v, w, graph);
        if (state == null)
            return;
        long found_subgraphs = 0;

        if (k < 9) {
            LongLongOpenHashMap freq_map = SubgraphEnumerator.enumerateStateHPPC(graph, state, k);
            for (int i = 0; i < freq_map.keys.length; i++) {
                if (freq_map.allocated[i]) {
                    long key = freq_map.keys[i];
                    key = BoolArray.boolArrayToLong(new SubGraphStructure(BoolArray.longToBoolArray(key, k * k)).getOrderedForm().getAdjacencyArray().getArray());
                    labelHPPCMap.putOrAdd(key, freq_map.values[i], freq_map.values[i]);
                    found_subgraphs += freq_map.values[i];
                }
            }
            context.getCounter(MRSUBCounters.Total_Labels).increment(freq_map.size());
        } else {
            FreqMap freq_map = SubgraphEnumerator.enumerateState(graph, state, k);
            for (Map.Entry<BoolArray, FreqMap.Count> entry : freq_map.map.entrySet()) {
                BoolArray key = new SubGraphStructure(entry.getKey().getArray()).getOrderedForm().getAdjacencyArray();
                labelMap.add(key, entry.getValue().get());
                found_subgraphs += entry.getValue().get();
            }

            context.getCounter(MRSUBCounters.Total_Labels).increment(freq_map.size());
        }

        context.getCounter(MRSUBCounters.Enumerated_SUBGRAPHS).increment(found_subgraphs);
        context.getCounter(MRSUBCounters.Finished_States).increment(1);
        flushMaps(context, flushTreshold);

    }
}
