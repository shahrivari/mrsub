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
import java.util.Stack;

/**
 * Created with IntelliJ IDEA.
 * User: saeed
 * Date: 5/5/13
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class SubMapperN
        extends Mapper<Text, IntWritable, Text, LongWritable> {
    Graph graph = null;
    int k = 0;
    //long foundSubgraphs =0;
    FreqMap labelMap = new FreqMap();
    LongLongOpenHashMap labelHPPCMap = new LongLongOpenHashMap(1024, 0.5f);
    int flushTreshold = 1024 * 1024;

    void flushMaps(Context context, int treshold) throws IOException, InterruptedException {
        context.progress();
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

    void addMap(LongLongOpenHashMap freq_map) {
        for (int i = 0; i < freq_map.keys.length; i++) {
            if (freq_map.allocated[i]) {
                long key = freq_map.keys[i];
                key = BoolArray.boolArrayToLong(new SubGraphStructure(BoolArray.longToBoolArray(key, k * k)).getOrderedForm().getAdjacencyArray().getArray());
                labelHPPCMap.putOrAdd(key, freq_map.values[i], freq_map.values[i]);
            }
        }
    }

    void addMap(FreqMap freq_map) {
        for (Map.Entry<BoolArray, FreqMap.Count> entry : freq_map.map.entrySet()) {
            BoolArray key = new SubGraphStructure(entry.getKey().getArray()).getOrderedForm().getAdjacencyArray();
            labelMap.add(key, entry.getValue().get());
        }
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        SubGraphStructure.beFast = context.getConfiguration().getBoolean("be_fast", false);
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
        long foundSubgraphs = 0;
        k = value.get();
        int v = Integer.parseInt(state_str.toString().split(" ")[0]);
        int w = Integer.parseInt(state_str.toString().split(" ")[1]);
        SMPState state = SMPState.makeState(v, w, graph);
        if (state == null)
            return;

        if (k < 9) {
            foundSubgraphs = enumerateStateHPPC(context, state);
        } else {
//            FreqMap freq_map=SubgraphEnumerator.enumerateState(graph, state, k);
//            for(Map.Entry<BoolArray,FreqMap.Count> entry : freq_map.map.entrySet()){
//                BoolArray key = new SubGraphStructure(entry.getKey().getArray()).getOrderedForm().getAdjacencyArray();
//                labelMap.add(key,entry.getValue().get());
//                foundSubgraphs+=entry.getValue().get();
//            }
//
//            context.getCounter(MRSUBCounters.Total_Labels).increment(freq_map.size());
        }

        context.getCounter(MRSUBCounters.Enumerated_SUBGRAPHS).increment(foundSubgraphs);
        context.getCounter(MRSUBCounters.Finished_States).increment(1);
        flushMaps(context, flushTreshold);

    }

    public long enumerateStateHPPC(Context context, SMPState init_state) throws IOException, InterruptedException {
        if (k > 8)
            throw new IllegalArgumentException("k must be smaller or equal to 8.");
        long foundSubgraphs = 0;
        LongLongOpenHashMap freqmap = new LongLongOpenHashMap(1024, 0.5f);
        Stack<SMPState> stack = new Stack<SMPState>();
        stack.push(init_state);
        int[] foundSubGraph = new int[k];

        while (stack.size() > 0) {
            SMPState state = stack.pop();
            if (state.subgraph.length >= k)
                throw new IllegalStateException("This must never HAPPEN!!!");

            while (!state.extension.isEmpty()) {
                int w = state.extension.get(state.extension.size() - 1);
                state.extension.remove(state.extension.size() - 1);
                if (state.subgraph.length == k - 1) {
                    System.arraycopy(state.subgraph, 0, foundSubGraph, 0, k - 1);
                    foundSubGraph[k - 1] = w;//state.extension[i];
                    long subl = graph.getSubGraphAsLong(foundSubGraph);
                    freqmap.putOrAdd(subl, 1, 1);
                    foundSubgraphs++;
                    if (freqmap.size() >= flushTreshold) {
                        addMap(freqmap);
                        freqmap.clear();
                        flushMaps(context, flushTreshold);
                    }
                } else {
                    SMPState new_state = state.expand(w, graph);
                    if (new_state.extension.size() > 0)
                        stack.add(new_state);
                }
            }
        }
        addMap(freqmap);
        freqmap.clear();
        flushMaps(context, flushTreshold);
        return foundSubgraphs;
    }

}
