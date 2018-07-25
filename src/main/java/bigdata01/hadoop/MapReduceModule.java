package bigdata01.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceModule{
    // create Mapper class
    public static class MapReduceMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text mapOutputKey = new Text();

        // map output value +1 when the key already exists
        private IntWritable mapOutputValue = new IntWritable(1);
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

            // Read line with file,transform Text type to String type
            String lineValues = value.toString();

            // Split word by " ", grouped <key,value>
            String[] strs = lineValues.split(" ");
            for(String str : strs){

                // Set key output
                mapOutputKey.set(str);

                // Set map output
                context.write(mapOutputKey,mapOutputValue);
            }
        }
    }


    // Create Reducer class
    public static class MapReduceReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable outputValue = new IntWritable();
        public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException {
           int sum = 0;
           for(IntWritable value:values){
               sum += value.get();
           }
           outputValue.set(sum);
           context.write(key, outputValue);
        }
    }

    // step: Driver
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Get configuration from cluster
        Configuration configuration = new Configuration();

        // Create a job
        Job job = Job.getInstance(configuration,this.getClass().getSimpleName());

        // Which class the application to run,
        // and the MapReduce will start from here.
        job.setJarByClass(this.getClass());

        // input file
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job,inpath);

        // output path
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        // Set mapper.
        job.setMapperClass(MapReduceMapper.class);
        job.setOutputKeyClass(Text.class);

        // Set reducer.
        job.setReducerClass(MapReduceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Submit job on yarn.
        Boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 在Windows上测试代码
//        args = new String[]{
//                "hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wc.input",
//                "hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wcout"
//        };

        int status = new MapReduceModule().run(args);

        System.exit(status);

        // make jar file and run it on yarn, this is command :
        // bin/yarn jar /opt/datas/spark01.jar hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wc.input hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wcout

        // check result:
        // bin/hdfs dfs -text /wc/wcout/*
    }
}
