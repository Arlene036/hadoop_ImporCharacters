package hadoop;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class WordCount {
    public static class Map extends Mapper<Object,Text,Text,IntWritable>{
        private static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            //Input key , output key, context = (Object key,Text value,Context context)
            StringTokenizer st = new StringTokenizer(value.toString());



            while(st.hasMoreTokens()){
                word.set(st.nextToken());
                context.write(word, one);//map returns nothing, but use context class to return what should be the output!!
            }
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        private static IntWritable result = new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
            for(IntWritable val:values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    static {
        try {
            System.load("D:\\hadoop\\hadoop2.7.7-master\\hadoop-2.7.7\\bin\\hadoop.dll");//建议采用绝对地址，bin目录下的hadoop.dll文件路径
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME","hadoop");
        System.setProperty("HADOOP_USER_PASSWORD","hadoop");
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境。
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        Job job = new Job(conf,"word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //   /pro1_input/其他国家（24本）/20,000 Leagues Under the Sea-海底两万里
//        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3])); //只能输出一个
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
