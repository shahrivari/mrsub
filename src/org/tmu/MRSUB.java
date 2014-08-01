package org.tmu;

import com.carrotsearch.hppc.LongLongOpenHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import com.google.common.base.Stopwatch;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.tmu.subdigger.*;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Saeed on 6/20/14.
 */
public class MRSUB extends Configured implements Tool {

    static private Path WORK_DIR;
    static private final int max_par = 5000;
    static String input_path = "";
    static boolean verbose = false;
    static int subgraph_size = 3;
    static CommandLineParser parser;
    static CommandLine commandLine;
    static HelpFormatter formatter = new HelpFormatter();
    static Options options = new Options();
    static Graph graph = null;

    private class Edge {
        int src;
        int dest;

        public Edge(int src, int dest) {
            this.src = src;
            this.dest = dest;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        Configuration conf = getConf();
        int nMaps = 1;
        int nReduces = 1;

        if (commandLine.hasOption("nm")) {
            nMaps = Integer.parseInt(commandLine.getOptionValue("nm"));
            if (nMaps < 1) {
                System.out.println("Number of map tasks must be greater or equal to 1.");
                System.exit(-1);
            }
        } else {
            System.out.println("Number of map tasks must be given.");
            formatter.printHelp("MRSUB", options);
            System.exit(-1);
        }

        if (commandLine.hasOption("nr")) {
            nReduces = Integer.parseInt(commandLine.getOptionValue("nr"));
            if (nMaps < 1) {
                System.out.println("Number of reduce tasks must be greater or equal to 1.");
                System.exit(-1);
            }
        }


        WORK_DIR = new Path(MRSUB.class.getSimpleName() + "_DIR_" + new Path(input_path).getName() + "_" + subgraph_size);

        final Path inDir = new Path(WORK_DIR, "in");
        final Path outDir = new Path(WORK_DIR, "out");


        final FileSystem fs = FileSystem.get(conf);
        if (fs.exists(WORK_DIR)) {
            if (!commandLine.hasOption("y")) {
                System.out.print("Work directory " + fs.makeQualified(WORK_DIR) + " already exists!  remove it first (y/n)?");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                String line = "";
                try {
                    line = br.readLine();
                } catch (IOException ioe) {
                    System.out.println("IO error trying to read from console!");
                    System.exit(1);
                }
                if (!line.toLowerCase().equals("y")) {
                    System.out.printf("You did not typed 'y'. Then I will quit!!!\n");
                    System.exit(1);
                }
                fs.delete(WORK_DIR, true);
            } else
                fs.delete(WORK_DIR, true);

        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }

        Path input_graph = new Path(WORK_DIR, "input_graph");
        fs.copyFromLocalFile(new Path(input_path), input_graph);
        System.out.println("Copied input graph to " + input_graph.toUri());


        List<Edge> edges = new ArrayList<Edge>();
        for (int v : graph.getVertices()) {
            for (int w : graph.getNeighbors(v)) {
                if (w < v) continue;
                edges.add(new Edge(v, w));
            }
        }

        if (nMaps > edges.size()) {
            nMaps = edges.size();
            System.out.printf("Reset number of maps to %,d because there are less edges.\n", nMaps);
        }

        List<List<Edge>> chunks = new ArrayList<List<Edge>>(nMaps);
        for (int i = 0; i < nMaps; i++)
            chunks.add(new ArrayList<Edge>());

        int list_i = 0;
        for (Edge edge : edges) {
            chunks.get(list_i).add(edge);
            list_i = (list_i + 1) % nMaps;
        }

        System.out.printf("Intermediate states: %,d\n", edges.size());

        System.out.print("Writing input for Map #:");
        for (int i = 0; i < chunks.size(); ++i) {
            final Path file = new Path(inDir, "part" + i);
            final IntWritable intWritable = new IntWritable(subgraph_size);
            final SequenceFile.Writer writer = SequenceFile.createWriter(
                    fs, conf, file,
                    Text.class, IntWritable.class);
            try {
                for (Edge edge : chunks.get(i))
                    writer.append(new Text(edge.src + " " + edge.dest), intWritable);
            } finally {
                writer.close();
            }
            if (i % 50 != 0)
                System.out.print(".");
            else
                System.out.print(i);
        }
        System.out.println(nMaps + ".");


//                //generate an input file for each map task
//        System.out.print("Writing input for Map #:");
//
//        List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>();
//        for (int i = 1; i < nMaps; ++i) {
//            Path file = new Path(inDir, "part" + i);
//            SequenceFile.Writer writer = SequenceFile.createWriter(
//                    fs, conf, file,
//                    Text.class, IntWritable.class);
//            writers.add(writer);
//        }
//
//        final IntWritable intWritable = new IntWritable(subgraph_size);
//        int writer_index = 0;
//        int state_count = 0;
//        int expanded=0;
//
//
//        for(int v:graph.getVertices()){
//            for(int w:graph.getNeighbors(v)){
//                if(w<v) continue;
//                writers.get(writer_index).append(new Text(v + " " + w), intWritable);
//                writer_index = (writer_index + 1) % writers.size();
//                if(++state_count%1000==0)System.out.printf("Written %,d states.\n",state_count);
//            }
//        }
//
//
//        for (SequenceFile.Writer writer : writers)
//            writer.close();
//
//        System.out.printf("Written states: %,d\n" , state_count);


        Job job = new Job(conf, "MRSUB-" + new Path(input_path).getName() + "-" + subgraph_size + "-" + nMaps + "-" + nReduces);
        job.setJarByClass(MRSUB.class);
        job.setMapperClass(SubMapperN.class);
        job.setCombinerClass(SubCombiner.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.getConfiguration().set("input_path", WORK_DIR + "/input_graph");
        job.getConfiguration().set("working_dir", WORK_DIR.toString());
        job.getConfiguration().set("mapred.output.compress", "true");
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().set("mapred.compress.map.output", "true");
        job.getConfiguration().set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().set("mapred.task.timeout", "36000000");
        FileInputFormat.addInputPath(job, inDir);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, outDir);
        System.out.println("Set Reduce tasks to " + nReduces);
        job.setNumReduceTasks(nReduces);


        int result = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("Cleaning created files....");
        System.out.printf("Took %s.\n", stopwatch);
        System.out.printf("Found %,d subgraphs.\n", job.getCounters().findCounter(MRSUBCounters.Enumerated_SUBGRAPHS).getValue());
        System.out.printf("Found %,d labels.\n", job.getCounters().findCounter(MRSUBCounters.Unique_Labels).getValue());
        fs.delete(inDir, true);
        return result;
    }

    public static void main(String[] args) throws Exception {
        // create the Options
        Stopwatch watch = new Stopwatch().start();
        options.addOption("nm", "nmap", true, "number of map tasks.");
        options.addOption("nr", "nreduce", true, "number of reduce tasks.");
        options.addOption("y", "overwrite", false, "overwrite output if exists.");
        options.addOption("v", "verbose", false, "verbose mode.");
        options.addOption("mp", "max-par", false, "use maximum parallelism for map tasks. If edges<" + max_par + " then mp=" + max_par + " else mp=#edges.");
        options.addOption("i", "input", true, "the input graph's file name.");
        options.addOption("s", "size", true, "size of subgraphs to enumerate.");
        options.addOption("l", "local", true, "run locally and dump labels to output file.");
        options.addOption("x", "max", true, "run for at most x seconds (just works in local mode).");

        parser = new BasicParser();
        commandLine = parser.parse(options, args);

        if (commandLine.hasOption("v"))
            verbose = true;

        if (commandLine.hasOption("s")) {
            subgraph_size = Integer.parseInt(commandLine.getOptionValue("s"));
            if (subgraph_size < 3) {
                System.out.println("Size of subgraphs must be greater or equal to 3.");
                System.exit(-1);
            }
        } else {
            System.out.println("Size of subgraphs must be given.");
            formatter.printHelp("MRSUB", options);
            System.exit(-1);
        }

        if (commandLine.hasOption("i")) {
            input_path = commandLine.getOptionValue("i");
        } else {
            System.out.println("Input file must be given.");
            formatter.printHelp("MRSUB", options);
            System.exit(-1);
        }

        graph = HashGraph.readStructureFromFile(input_path);
        if (graph.vertexCount() < 15000)
            graph = MatGraph.readStructureFromFile(input_path);

        System.out.println("Graph loaded: " + input_path);

        if (commandLine.hasOption("l")) {
            long max_time = Long.MAX_VALUE;
            System.out.println("Subgraphs size: " + subgraph_size);
            if (commandLine.hasOption("x"))
                max_time = Long.parseLong(commandLine.getOptionValue("x"));
            String output_path = commandLine.getOptionValue("l");
            enumerateInLocal(graph, subgraph_size, output_path, max_time);
            System.out.printf("Took %s equal to %,d seconds\n", watch, watch.elapsed(TimeUnit.SECONDS));
            System.exit(0);
        }

        System.exit(ToolRunner.run(null, new MRSUB(), args));
    }

    private static void enumerateInLocal(Graph g, int k, String output_path, long max_time) throws IOException {
        if (k > 8) {
            System.out.println("Subgraph size must be smaller than 9.");
            throw new IllegalArgumentException("Subgraph size must be smaller than 9.");
        }
        Stopwatch stopwatch = Stopwatch.createStarted();

        LongLongOpenHashMap result = new LongLongOpenHashMap();

        long total_subgraphs = 0;
        for (int v : graph.getVertices()) {
            SMPState state = new SMPState(v, graph.getNeighbors(v));
            LongLongOpenHashMap res = SubgraphEnumerator.enumerateStateHPPC(g, state, k);
            for (LongLongCursor iter : res) {
                long key = iter.key;
                key = BoolArray.boolArrayToLong(new SubGraphStructure(BoolArray.longToBoolArray(key, k * k)).getOrderedForm().getAdjacencyArray().getArray());
                result.putOrAdd(key, iter.value, iter.value);
                total_subgraphs += iter.value;
            }
            long elapsed = stopwatch.elapsed(TimeUnit.SECONDS);
            if (verbose)
                System.out.printf("Elapsed %,d seconds; found %,d subgraphs, and %,d labels.\n", elapsed, total_subgraphs, result.size());
            if (elapsed > max_time)
                break;
        }

        FileWriter writer = new FileWriter(output_path);
        for (LongLongCursor iter : result) {
            writer.write(Long.toString(iter.key, 32) + "\t" + iter.value + "\n");
        }
        System.out.printf("Total subgraphs: %,d\n", total_subgraphs);
        System.out.printf("Unique labels: %,d\n", result.size());
        writer.close();
    }


}
