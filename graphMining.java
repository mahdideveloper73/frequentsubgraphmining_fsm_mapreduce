
import java.util.*;
import java.io.*;
import java.lang.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
//import com.hadoop.compression.lzo.LzopCodec;

public class graphMining extends Configured implements Tool {


    public static class mapperMiner extends MapReduceBase implements Mapper<Text, BytesWritable, Text, MapWritable> {

        private JobConf conf;

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;

        }

        public void map(Text key, BytesWritable value, OutputCollector<Text, MapWritable> output, Reporter reporter) throws IOException {
            long start = System.currentTimeMillis();

            Text map_value = new Text();

            MR_Serialize Serial = null;

            try {

                ByteArrayInputStream sbufIn = new ByteArrayInputStream(value.getBytes());
                ObjectInputStream in = new ObjectInputStream(sbufIn);
                Serial = (MR_Serialize) in.readObject();
                in.close();
                sbufIn.close();
            } catch (ClassNotFoundException c) {
                System.out.println("pattern class not found.");
            }
            MR_Pattern P = new MR_Pattern();

            Util_functions func1 = new Util_functions();
            func1.l1vat = Serial.l1vat;
            func1.l1map_v2 = Serial.l1map_v2;
            P = Serial.Pattern;

            func1.generate_candidate(P);

            for (int i = 0; i < func1.freq_pats_hadoop.size(); i++) {
                if (((MR_Pattern) (func1.freq_pats_hadoop.toArray()[i])).can_cod.size() == P.can_cod.size())
                    continue;
                String key1 = ((MR_Pattern) (func1.freq_pats_hadoop.toArray()[i])).getCan_code();

                MR_Serialize Serial_ = new MR_Serialize();
                Serial_.Pattern = (MR_Pattern) (func1.freq_pats_hadoop.toArray()[i]);
                Serial_.l1map_v2 = func1.l1map_v2;
                Serial_.l1vat = func1.l1vat;

                IntWritable sup = new IntWritable(Serial_.Pattern.support);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {

                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(Serial_);
                    oos.flush();
                    oos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                MapWritable m = new MapWritable();
                m.put(sup, new BytesWritable(baos.toByteArray()));

                Text map_key = new Text();
                map_key.set(key1);

                output.collect(map_key, m);

            }

            func1.l1map_v2.clear();
            func1.l1vat.clear();
            func1.check_unique.clear();

        }
    }

    public static class reducerMiner extends MapReduceBase implements Reducer<Text, MapWritable, Text, BytesWritable> {

        private int minsup;
        private boolean keepMovingflag;

        public void configure(JobConf job) {
            minsup = Integer.parseInt(job.get("minsup"));

        }

        public void reduce(Text symbol, Iterator<MapWritable> values, OutputCollector<Text, BytesWritable> output, Reporter reporter)
                throws IOException {

            int support = 0;
            int i = 0;
            int j = 0;


            List<BytesWritable> hold_value = new ArrayList();

            while (values.hasNext()) {

                MapWritable value = values.next();

                IntWritable map_k = (IntWritable) (((Set) (value.keySet())).toArray()[0]);

                support += map_k.get();

                hold_value.add((BytesWritable) (value.get(map_k)));
                i++;


            }

            if (support >= minsup) {
                reporter.incrCounter(MRStats.NUMBER_OF_PATTEN_EXTENDED, 1);
                reporter.incrCounter(MRStats.NUMBER_OF_PATTEN, 1);
                IntWritable index = new IntWritable(0);
                for (j = 0; j < hold_value.size(); j++) {
                    output.collect(symbol, hold_value.get(j));
                }
            } else {
                //reporter.incrCounter(MRStats.NUMBER_REJECTED, 1);
            }

        }


    }

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: " + " <in> <output> <minsup><no_of_mapper><no_of_reducer>");

            return -1;
        }
        String output_dir = args[1];
        long total_pattern = 0;
        String minsup = args[2];

        JobConf conf1 = new JobConf(getConf(), graphMining.class);

        conf1.setInputFormat(MyInputFormat.class);
        conf1.setMapperClass(mapperReader.class);
        conf1.setReducerClass(reducerReader.class);

        conf1.setMapOutputKeyClass(Text.class);
        conf1.setMapOutputValueClass(BytesWritable.class);
        conf1.setOutputKeyClass(Text.class);

        conf1.setOutputValueClass(BytesWritable.class);
        conf1.setOutputFormat(SequenceFileOutputFormat.class);


        conf1.set("mapred.task.timeout", "7200");

        conf1.set("mapred.output.compress", "true");
        conf1.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        conf1.set("minsup", minsup);

        conf1.setNumMapTasks(Integer.parseInt(args[3]));
        conf1.setNumReduceTasks(Integer.parseInt(args[4]));

        FileInputFormat.addInputPaths(conf1, args[0]);

        FileOutputFormat.setOutputPath(conf1, new Path(output_dir + "/temp"));

        long start = System.currentTimeMillis();
        RunningJob job1 = JobClient.runJob(conf1);
        Counters counters1 = job1.getCounters();
        total_pattern = total_pattern + counters1.getCounter(MRStats.NUMBER_OF_PATTEN);
        int iterationCount = 0;

        while (true) {

            String input;
            if (iterationCount == 0)
                input = output_dir + "/temp";
            else
                input = output_dir + "/output-graph-" + iterationCount;

            String output = output_dir + "/output-graph-" + (iterationCount + 1);

            JobConf conf2 = new JobConf(getConf(), graphMining.class);

            conf2.setMapperClass(mapperMiner.class);
            conf2.setReducerClass(reducerMiner.class);

            conf2.setInputFormat(SequenceFileInputFormat.class);
            conf2.setOutputFormat(SequenceFileOutputFormat.class);
            conf2.setOutputValueClass(BytesWritable.class);

            conf2.setMapOutputKeyClass(Text.class);
            conf2.setMapOutputValueClass(MapWritable.class);
            conf2.setOutputKeyClass(Text.class);

            conf2.set("mapred.task.timeout", "1800000000");
            conf2.set("mapred.output.compress", "true");
            conf2.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
            conf2.set("minsup", minsup);

            conf2.setNumMapTasks(Integer.parseInt(args[3]));
            conf2.setNumReduceTasks(Integer.parseInt(args[4]));
            FileInputFormat.addInputPath(conf2, new Path(input));
            FileOutputFormat.setOutputPath(conf2, new Path(output));

            RunningJob job = JobClient.runJob(conf2);
            Counters counters = job.getCounters();
            long pattern_extended = counters.getCounter(MRStats.NUMBER_OF_PATTEN_EXTENDED);
            total_pattern = total_pattern + counters.getCounter(MRStats.NUMBER_OF_PATTEN);
            if (pattern_extended == 0)
                break;
            iterationCount++;
        }
        long end = System.currentTimeMillis();
        System.out.println("running time " + (end - start) / 1000 + "s");
        System.out.println("total_pattern " + total_pattern);
        OutPutFileReader.printResult();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new graphMining(), args));
    }


}
