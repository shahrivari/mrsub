package org.tmu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: saeed
 * Date: 5/5/13
 * Time: 2:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class LongSumReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
//        if (key.equals(Util.simpleSubgraphCountTextKey)) {
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            Path outFile = new Path(context.getConfiguration().get("working_dir") + "/counts");
//            if (!fs.exists(outFile)) {
//                //throw new IOException("Cannot create output file: " + outFile.toString());
//                FSDataOutputStream out = fs.create(outFile);
//                OutputStreamWriter writer = new OutputStreamWriter(out);
//                for (LongWritable val : values)
//                    writer.write(val.toString() + "\n");
//                writer.close();
//                out.close();
//            }
//        } else {
//            if (key.equals(Util.simpleChunkSubgraphCountTextKey)) {
//                FileSystem fs = FileSystem.get(context.getConfiguration());
//                Path outFile = new Path(context.getConfiguration().get("working_dir") + "/counts_per_chunk");
//                if (!fs.exists(outFile)) {
//                    //throw new IOException("Cannot create output file: " + outFile.toString());
//                    FSDataOutputStream out = fs.create(outFile);
//                    OutputStreamWriter writer = new OutputStreamWriter(out);
//                    for (LongWritable val : values)
//                        writer.write(val.toString() + "\n");
//                    writer.close();
//                    out.close();
//                }
//            } else {
        context.getCounter(MRSUBCounters.Unique_Labels).increment(1);
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
//            }
//        }
    }
}
