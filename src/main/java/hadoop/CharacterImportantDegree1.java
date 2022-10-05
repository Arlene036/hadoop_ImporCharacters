package hadoop;

import java.io.*;
import java.util.*;


import hadoop.fileinputformat.WholeFileInputformat;
import hadoop.keyvalueClass.BookCharResult;
import hadoop.keyvalueClass.ChapAppearNeighbor;
import hadoop.keyvalueClass.appearTimeNeighbor;
import hadoop.keyvalueClass.CharacterImportantDegree;
import hadoop.network.GetNodeRank;
import hadoop.topK.CharacterImportanceNode;
import hadoop.topK.GetTopK;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.graphstream.graph.Graph;

import static java.lang.Math.max;
import static java.lang.Math.sqrt;

public class CharacterImportantDegree1 {

    private static Tokenizer tokenizer;
    private static NameFinderME nameFinder;

    private static List<String> not_Name = new ArrayList<String>();

    public static boolean isName(String input) {
        if (!input.matches("[a-zA-Z]+") || not_Name.contains(input)) {
            return false;
        }
        return true;
    }

    public CharacterImportantDegree1() throws IOException {

        InputStream isTok = new FileInputStream("opennlpmodel/en-token.bin");
        TokenizerModel modelTok = new TokenizerModel(isTok);
        tokenizer = new TokenizerME(modelTok);
        isTok.close();

        //名字识别
        String modelPath = "opennlpmodel/en-ner-person.bin";
        InputStream isNF = new FileInputStream(modelPath);
        TokenNameFinderModel model = new TokenNameFinderModel(isNF);
        nameFinder = new NameFinderME(model);

        //名字判断
        not_Name.add("Lord");
        not_Name.add("Dear");
        not_Name.add("Oh");
        not_Name.add("They");
        not_Name.add("The");
        not_Name.add("That");
        not_Name.add("This");
        not_Name.add("For");
        not_Name.add("Ah");
        not_Name.add("And");
        not_Name.add("By");
        not_Name.add("All");
        not_Name.add("At");
        not_Name.add("Ask");
        not_Name.add("Do");
        not_Name.add("He");
        not_Name.add("She");
        not_Name.add("I");
        not_Name.add("Of");
        not_Name.add("So");
        not_Name.add("But");
        not_Name.add("Miss");
        not_Name.add("Mss");
        not_Name.add("Mr");
        not_Name.add("Mrs");
        not_Name.add("Mister");
        not_Name.add("If");
        not_Name.add("It");
        not_Name.add("In");
        not_Name.add("Consequently");
        not_Name.add("We");
        not_Name.add("Will");
        not_Name.add("Short");
    }

    private static List<String> parse(String text) throws IOException {
        List<String> words = new ArrayList<String>();
        String tokens[] = tokenizer.tokenize(text);

        //名字识别
        Span[] nameFinds = nameFinder.find(tokens);

        for (Span str : nameFinds) {
            String n = tokens[str.getStart()];
            if (isName(n)) {
                words.add(n);
            }
        }

        return words;
    }

    public static class MapStep1 extends Mapper<Text, Text, Text, appearTimeNeighbor> {
        private Text outputKey = new Text();
        private appearTimeNeighbor atn = new appearTimeNeighbor();

        //Input key , output key, context = (Object key,Text value,Context context)
        //读进来的value是一整篇的文字，用value.toString()变成String类型处理；
        //第一行是书名，找CHAPTER ___，找人名
        //temp list：String[] 一段中出现的所有人名
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //key: hdfs://192.168.19.128:9000/pro1_input/英国（56本）/A Christmas Carol-圣诞欢歌/A CHRISTMAS CAROL-C02.txt
            String[] file_info = key.toString().split("/");
            String bookName = file_info[file_info.length - 2];
            String chapName = file_info[file_info.length - 1];

            String[] lines = value.toString().split("\n");


            List<String> names = parse(value.toString());

            Set<String> names_Set = new HashSet<String>();
            for (String name : names) {
                names_Set.add(name);
            }


            //todo, this is a O(n^2) method to find neighbors! OMG!
            for (String name : names) {
                List<Text> neighbor = new ArrayList<Text>();
                for (String name1 : names_Set) {
                    if (!name1.equals(name)) {
                        neighbor.add(new Text(name1));
                    }
                }

                atn.setValue(neighbor);
                atn.setAppear_times(1);
                key.set(bookName + "\t" + chapName + "\t" + name);

                context.write(key, atn);
            }

        }
    }

    public static class ReduceStep1 extends Reducer<Text, appearTimeNeighbor, Text, appearTimeNeighbor> {

        private appearTimeNeighbor atn = new appearTimeNeighbor();

        @Override
        public void reduce(Text key, Iterable<appearTimeNeighbor> values, Context context) throws IOException, InterruptedException {

            atn.setValue(new ArrayList<Text>());
            atn.setAppear_times(0);
            int count = 0;

            for (appearTimeNeighbor apt_ng : values) {
                atn.add(apt_ng);
                count++;
            }
            context.write(key, atn);
        }
    }

    public static class MapStep2 extends Mapper<Object, Text, Text, ChapAppearNeighbor> {
        private Text output_key = new Text();
        private ChapAppearNeighbor can = new ChapAppearNeighbor();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            //todo 可以改成一行一行读入
//            String v = value.toString();
//            String[] lines = v.split("\n");
//            for (String line : lines) {
//                String[] vals = line.split("\t");
//
//                output_key.set(vals[0] + "\t" + vals[2]);
//
//                can.setValue_appear(new IntWritable(Integer.parseInt(vals[3])));
//
//                String[] neighbors = new String[0];
//                if (vals.length >= 5) {
//                    neighbors = vals[4].split(",");
//                }
//
//                can.setNeighbors(neighbors);
//
//                context.write(output_key, can);
//            }

                String[] vals = value.toString().split("\t");

                output_key.set(vals[0] + "\t" + vals[2]);

                can.setValue_appear(new IntWritable(Integer.parseInt(vals[3])));

                String[] neighbors = new String[0];
                if (vals.length >= 5) {
                    neighbors = vals[4].split(",");
                }

                can.setNeighbors(neighbors);

                context.write(output_key, can);

        }
    }

    public static class ReduceStep2 extends Reducer<Text, ChapAppearNeighbor, Text, BookCharResult> {
        private BookCharResult bcr = new BookCharResult();
        @Override
        public void reduce(Text key, Iterable<ChapAppearNeighbor> values, Context context) throws IOException, InterruptedException {
            List<Text> neighbors = new ArrayList<Text>();

            ArrayList<Integer> appearTimesList = new ArrayList<Integer>(); //
            for (ChapAppearNeighbor vn : values) {
                neighbors.removeAll(vn.getNeighbors());
                neighbors.addAll(vn.getNeighbors());
                appearTimesList.add(vn.getValue_appear().get());
            }

            String[] key_list = key.toString().split("\t");
            int m = Integer.parseInt(key_list[0].split(",")[1]); //这本书有的chapter数量
            int sum = 0;
            int max = appearTimesList.get(0);

            boolean min_eq_0 = false;
            int min = appearTimesList.get(0);
            if (m > appearTimesList.size()) {
                min_eq_0 = true;
                min = 0;
            }
            for (int i = 0; i < appearTimesList.size(); i++) { //求和
                max = appearTimesList.get(i) > max ? appearTimesList.get(i) : max;
                if (!min_eq_0) {
                    min = appearTimesList.get(i) < min ? appearTimesList.get(i) : min;
                }
                sum += appearTimesList.get(i);
            }
            int range = max - min; //极差
            double mean = (sum * 1.0) / m; //求平均值

            double dVar = 0;
            for (int i = 0; i < m; i++) { //求方差
                if (i <= appearTimesList.size() - 1) {
                    dVar += (appearTimesList.get(i) - mean) * (appearTimesList.get(i) - mean);
                } else {
                    dVar += mean * mean;
                }
            }

            double std = sqrt(dVar / (m - 1));
            std = Double.parseDouble(String.format("%.3f", std)); //保留三位小数

            bcr.setNeighbors(neighbors);
            bcr.setMean(mean);
            bcr.setRange(range);
            bcr.setStd(std);
            bcr.setSum(sum);

            context.write(key, bcr);
        }
    }

    //job3
    public static class MapStep3 extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            outputKey.set(data[0]);

            StringBuffer sb = new StringBuffer();
            for (int i = 1; i < data.length; i++) {
                sb.append(data[i]);
                if (i != data.length - 1) {
                    sb.append("\t");
                }
            }
            outputValue.set(sb.toString());
//            System.out.println(sb.toString()+"----------------main line 276 -------------------"); ok!

            context.write(outputKey, outputValue);

        }
    }


    public static class ReduceStep3 extends Reducer<Text, Text, Text, Text> {

        //input value format (each value in values)
        //Ali	2	0.4	0.894	2	Roger,Poor,Fezziwig,Jack,Robin,
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Text> output = new ArrayList<Text>();

            StringBuffer neighbor_info = new StringBuffer();

            int count = 0;
            double max_mean = 0;
            List<Text> values_copy = new ArrayList<Text>();
            for (Text v : values) {
                String[] vs = v.toString().split("\t");
                if(Integer.parseInt(vs[1])<=1 || vs.length<6){
                    continue; //过滤掉只出现一次的东西,过滤掉没有邻居的人
                }

                double mean = Double.parseDouble(vs[2]);
                if (count == 0) {
                    max_mean = mean;
                } else if (max_mean < mean) {
                    max_mean = mean;
                }

                neighbor_info.append(vs[0] + "\t");
                neighbor_info.append(vs[5] + "\n");

                count++;

                values_copy.add(new Text(v.toString()));
            }

            Graph g = GetNodeRank.formGraph(neighbor_info.toString());


            for (int i = 0 ; i < values_copy.size() ; i ++) {
                Text v = values_copy.get(i);
                String[] vs = v.toString().split("\t");

                double rank = GetNodeRank.getNodeRank(g, vs[0]);
                double mean = Double.parseDouble(vs[2]);
                double std = Double.parseDouble(vs[3]);
                double range = Double.parseDouble(vs[4]);

                double score = 30 * rank + 70 * (0.9 * (mean / max_mean) + 0.05 * (1 / (std + 1)) + 0.05 * (1 / (range + 1)));

                //todo judge whether top k
                output.add(new Text(vs[0]));
                output.add(new Text(String.format("%.3f", score)));
            }

            StringBuffer sb = new StringBuffer();
            for (int i = 0 ; i < output.size() ; i++){
                sb.append(output.get(i).toString()+"\t");
            }
            context.write(key,new Text(sb.toString()));

        }
    }


    //job 4

    public static class MapStep4 extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        @Override
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            String[] values = value.toString().split("\t");
            outputKey.set(values[0]);


            List<CharacterImportanceNode> nums = new ArrayList<>();
            for (int i = 1 ; i < values.length ; i+=2){
                CharacterImportanceNode temp = new CharacterImportanceNode(values[i],Double.parseDouble(values[i+1]));
                nums.add(temp);
            }

            List<CharacterImportanceNode> top3 = GetTopK.topKMax(nums,3);

            StringBuffer sb = new StringBuffer();
            for (int i = 0 ; i<top3.size() ; i ++){
                sb.append(top3.get(i).getName()+"\t");
                sb.append(top3.get(i).getScore());
                if(i!=top3.size()-1){
                    sb.append("\t");
                }
            }

            context.write(outputKey,new Text(sb.toString()));
        }

    }

    public static class ReduceStep4 extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values){
                outputValue.set(v);
            }
            context.write(key,outputValue);
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


    public static void main(String[] args) throws Exception {
        CharacterImportantDegree1 c = new CharacterImportantDegree1();
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("HADOOP_USER_PASSWORD", "hadoop");
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境


        //第一个reduce的数据输出类型要与第二个reduce数据的输入类型一样
        JobConf conf = new JobConf(CharacterImportantDegree.class);


        //第一个job的配置----------------------------------------------------------------------------------------------------------------------
        Job job1 = new Job(conf, "Join1");
        job1.setJarByClass(CharacterImportantDegree.class);

        job1.setMapperClass(MapStep1.class);
        job1.setReducerClass(ReduceStep1.class);

        //mapper和reducer输出类型一致
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(appearTimeNeighbor.class);

        // 设置输入的inputFormat，一次性输入整个文件
        job1.setInputFormatClass(WholeFileInputformat.class);

        //job-1 加入控制容器
        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job1);

        //job-1 的输入输出文件路径

        // 路径
        String path = "E:\\Uni\\work\\grade3.1\\distributed memory and parallel compute\\英文原版世界名著100本（TXT）\\20books_original";
//        String path = "E:\\Uni\\work\\grade3.1\\distributed memory and parallel compute\\英文原版世界名著100本（TXT）\\20books";
        File f = new File(path);

        // 路径不存在
        if (!f.exists()) {
            System.out.println(path + " not exists");
            return;
        }

        File result[] = f.listFiles();

        for (int i = 0; i < result.length; i++) {
            FileInputFormat.addInputPath(job1, new Path("/pro1_input_1/" + result[i].getName()));
//            FileInputFormat.addInputPath(job1, new Path("/pro1_input_20books/" + result[i].getName()));
        }

        FileOutputFormat.setOutputPath(job1, new Path("/pro1_output/job1"));


        //第二个job的配置----------------------------------------------------------------------------------------------------------------------
        Job job2 = new Job(conf, "Join2");
        job2.setJarByClass(CharacterImportantDegree.class);


        job2.setMapperClass(MapStep2.class);
        job2.setReducerClass(ReduceStep2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(ChapAppearNeighbor.class);

//        // 设置输入的inputFormat，一次性输入整个文件
//        job2.setInputFormatClass(WholeFileInputformat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(BookCharResult.class);

        //job-2 加入控制容器
        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);

        //第三个job的配置----------------------------------------------------------------------------------------------------------------------
        Job job3 = new Job(conf, "Join3");
        job3.setJarByClass(CharacterImportantDegree.class);

        job3.setMapperClass(MapStep3.class);
        job3.setReducerClass(ReduceStep3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        //job-3 加入控制容器
        ControlledJob ctrljob3 = new ControlledJob(conf);
        ctrljob3.setJob(job3);

        //第四个job的配置----------------------------------------------------------------------------------------------------------------------
        //todo
        Job job4 = new Job(conf, "Join4");
        job4.setJarByClass(CharacterImportantDegree.class);

        job4.setMapperClass(MapStep4.class);
        job4.setReducerClass(ReduceStep4.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        //job-3 加入控制容器
        ControlledJob ctrljob4 = new ControlledJob(conf);
        ctrljob4.setJob(job4);

        //设置多个作业直接的依赖关系----------------------------------------------------------------------------------------------------------------

        //job-2 的启动，依赖于job-1作业的完成
        ctrljob2.addDependingJob(ctrljob1);
        ctrljob3.addDependingJob(ctrljob2);
        ctrljob4.addDependingJob(ctrljob3);


        //输出路径从新传入一个参数，这里需要注意，因为我们最后的输出文件一定要是没有出现过得
        //因此我们在这里new Path(args[2])因为args[2]在上面没有用过，只要和上面不同就可以了
        FileOutputFormat.setOutputPath(job2, new Path("/pro1_output/job2"));
        FileOutputFormat.setOutputPath(job3, new Path("/pro1_output/job3"));
        FileOutputFormat.setOutputPath(job4, new Path("/pro1_output/job4"));

        //输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
        FileInputFormat.addInputPath(job2, new Path("/pro1_output/job1"));
        FileInputFormat.addInputPath(job3, new Path("/pro1_output/job2"));
        FileInputFormat.addInputPath(job4, new Path("/pro1_output/job3"));

        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("CountjobCtrl");

        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);
        jobCtrl.addJob(ctrljob3);
        jobCtrl.addJob(ctrljob4);

        //在线程启动，记住一定要有这个
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            if (jobCtrl.allFinished()) {
                //如果作业成功完成，就打印成功作业的信息
                System.out.println(jobCtrl.getSuccessfulJobList());
                System.out.println("job all finished!!!!!!!~~~~~~~~~~~~~");
                jobCtrl.stop();
                break;
            }
        }


    }
}
