# hadoopPro1Q2
 Find important characters based on appear times per chapter and the relationship network



## Background

1. 目的：探寻**第三人称**英国小说的人物重要程度，以各章节活跃度、人物关系网为依据
2. 择书
   - 选择chapter数大于1的书，
   - 各个chapter之间是连贯的（比如莎士比亚散文集就被排除了）
   - 最好是第三人称的
3. 结果展示
   - 以一本书为例
     - 评分过程可视化（图、sum/mean/std/range）
     - 最后的榜单（在数据分析时取top即可）
   - 所有书的中心人物（第一名）展示





## Pre-requisite

#### 英文人名提取

https://zhuanlan.zhihu.com/p/461723946

https://www.noveltech.dev/python-nlp-find-names/

https://gitee.com/ljmeng/NER4Novel/tree/master  直接使用hanlp训练好的模型，无专门自定义人名字典 自动人名统计，部分人名简称转换 



用hadoop统计名字出现次数

https://www.cnblogs.com/Decmber/p/5491887.html



##### OpenNLP NameFinder

I used this

https://codeleading.com/article/75835062548/

官方文件：https://opennlp.apache.org/docs/2.0.0/manual/opennlp.html#tools.namefind



#### 人物重要程度排序

以一本小说为单位，每个章节为子单位。

1. 每个章节出现的次数 -> sum、mean、std、range

2. 人物关系网

加权排名



#### 多个mapreduce流程

##### 1.链式mapreduce

 `ChainMapper`

`ChainReducer`

[MapReduce进阶之链式MapReduce操作实战](https://blog.csdn.net/qq_41955099/article/details/93327278?spm=1001.2101.3001.6650.7&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7EBlogCommendFromBaidu%7ERate-7-93327278-blog-114626824.pc_relevant_aa_2&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7EBlogCommendFromBaidu%7ERate-7-93327278-blog-114626824.pc_relevant_aa_2&utm_relevant_index=8)

 **在一个MapReduce作业中可以存在多个Map类，但是至多只能存在一个Reducer类，且可以在Reduce操作后继续执行Map操作** 



##### 2.多个job串联

```java
JobConf conf = new JobConf(CountDriver.class);
//------------------------------------------------------------------------------
        //第一个job的配置
        Job job1 = new Job(conf, "Join1");
        job1.setJarByClass(CountDriver.class);

        job1.setMapperClass(CountMapstep1.class);
        job1.setReducerClass(CountReduceStep1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        //job-1 加入控制容器
        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job1);

        //job-1 的输入输出文件路径
        FileInputFormat.addInputPath(job1, new Path("输入的数据路径"));
        FileOutputFormat.setOutputPath(job1, new Path("数据输出的路径"));

//------------------------------------------------------------------------------
        //第二个job的配置
        Job job2 = new Job(conf, "Join2");
        job2.setJarByClass(CountDriver.class);

        job2.setMapperClass(CountMapstep2.class);
        job2.setReducerClass(CountReduceStep2.class);

        // 指定mapper输出类型和reducer输出类型
        // 由于在wordcount中mapper和reducer的输出类型一致，
        // 所以使用setOutputKeyClass和setOutputValueClass方法可以同时设定mapper和reducer的输出类型
        // 如果mapper和reducer的输出类型不一致时，可以使用setMapOutputKeyClass和setMapOutputValueClass单独设置mapper的输出类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        //job-2 加入控制容器
        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);

//------------------------------------------------------------------------------
        //设置多个作业直接的依赖关系
        //job-2 的启动，依赖于job-1作业的完成
        ctrljob2.addDependingJob(ctrljob1);

        //输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
        FileInputFormat.addInputPath(job2, new Path("上一个reduce的输出路径"));

        //输出路径从新传入一个参数，这里需要注意，因为我们最后的输出文件一定要是没有出现过得
        //因此我们在这里new Path(args[2])因为args[2]在上面没有用过，只要和上面不同就可以了
        FileOutputFormat.setOutputPath(job2, new Path("数据输出路径"));

        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("CountjobCtrl");

        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);

        //在线程启动，记住一定要有这个
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            if (jobCtrl.allFinished()) {
                //如果作业成功完成，就打印成功作业的信息
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }


```

https://blog.csdn.net/qq_45798620/article/details/109630083





#### 自定义OutputFormat（没用到）

多个文件输出



根据需求，我们要将一个输入文件中的包含 hadoop 单词的数据放在一个输出文件中，不包含hadoop单词的数据放在另外一个输出文件中 https://blog.csdn.net/qq_45834006/article/details/109287982



https://blog.csdn.net/baidu_41833099/article/details/121707175?spm=1001.2101.3001.6661.1&utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromBaidu%7ERate-1-121707175-blog-84366610.pc_relevant_3mothn_strategy_and_data_recovery&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromBaidu%7ERate-1-121707175-blog-84366610.pc_relevant_3mothn_strategy_and_data_recovery&utm_relevant_index=1



## Debugging

#### Writable类

map的key = reduce的key

map的key类型一定要是`WritableComparable`? YES!



【想要key是StringList类型】





[ObjectWritable](https://www.cnblogs.com/wuyudong/p/hadoop-writable.html)

针对Java基本类型、字符串、枚举、Writable、空值、Writable的其他子类，ObjectWritable提供了一个封装，适用于字段需要使用多种类型。 

 ObjectWritable作为一种通用机制，相当浪费资源，它需要为每一个输出写入封装类型的名字。如果类型的数量不是很多，而且可以事先知道，则可以使用一个静态类型数组来提高效率，并使用数组索引作为类型的序列化引用。GenericWritable就是因为这个目的被引入org.apache.hadoop.io包中。 

[GenericWritable](https://blog.csdn.net/woshisap/article/details/42066875)







#### 1003 

```java
ava.io.EOFException: Premature EOF: no length prefix available
	at org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed(PBHelper.java:2282)
	at org.apache.hadoop.hdfs.RemoteBlockReader2.newBlockReader(RemoteBlockReader2.java:423)
	at org.apache.hadoop.hdfs.BlockReaderFactory.getRemoteBlockReader(BlockReaderFactory.java:818)
	at org.apache.hadoop.hdfs.BlockReaderFactory.getRemoteBlockReaderFromTcp(BlockReaderFactory.java:697)
	at org.apache.hadoop.hdfs.BlockReaderFactory.build(BlockReaderFactory.java:355)
	at org.apache.hadoop.hdfs.DFSInputStream.blockSeekTo(DFSInputStream.java:673)
	at org.apache.hadoop.hdfs.DFSInputStream.readWithStrategy(DFSInputStream.java:882)
	at org.apache.hadoop.hdfs.DFSInputStream.read(DFSInputStream.java:934)
	at java.io.DataInputStream.read(DataInputStream.java:149)
	at org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader.fillBuffer(UncompressedSplitLineReader.java:62)
	at org.apache.hadoop.util.LineReader.readDefaultLine(LineReader.java:216)
	at org.apache.hadoop.util.LineReader.readLine(LineReader.java:174)
	at org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader.readLine(UncompressedSplitLineReader.java:94)
	at org.apache.hadoop.mapreduce.lib.input.LineRecordReader.skipUtfByteOrderMark(LineRecordReader.java:144)
	at org.apache.hadoop.mapreduce.lib.input.LineRecordReader.nextKeyValue(LineRecordReader.java:184)
	at org.apache.hadoop.mapred.MapTask$NewTrackingRecordReader.nextKeyValue(MapTask.java:556)
	at org.apache.hadoop.mapreduce.task.MapContextImpl.nextKeyValue(MapContextImpl.java:80)
	at org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context.nextKeyValue(WrappedMapper.java:91)
	at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:145)
	at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:787)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:341)
	at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:243)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
```



**忘记设置`job.setMapperClass`！**

```
java.lang.Exception: java.io.IOException: Type mismatch in key from map: expected org.apache.hadoop.io.Text, received hadoop.BookChapCharName
	at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:462)
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:522)
Caused by: java.io.IOException: Type mismatch in key from map: expected org.apache.hadoop.io.Text, received hadoop.BookChapCharName
	at org.apache.hadoop.mapred.MapTask$MapOutputBuffer.collect(MapTask.java:1074)
	at org.apache.hadoop.mapred.MapTask$NewOutputCollector.write(MapTask.java:715)
	at org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl.write(TaskInputOutputContextImpl.java:89)
	at org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context.write(WrappedMapper.java:112)
	at hadoop.CharacterImportantDegree$MapStep1.map(CharacterImportantDegree.java:94)
	at hadoop.CharacterImportantDegree$MapStep1.map(CharacterImportantDegree.java:59)
```





**运行时间太久：**

NLP模型训练太多次，改到构造器里，并且在main函数里面加上构造器

```java
   private static Tokenizer tokenizer;
    private static NameFinderME nameFinder;


    public CharacterImportantDegree() throws IOException {

        InputStream isTok = new FileInputStream("opennlpmodel/en-token.bin");
        TokenizerModel modelTok = new TokenizerModel(isTok);
        tokenizer = new TokenizerME(modelTok);
        isTok.close();

        //名字识别
        String modelPath = "opennlpmodel/en-ner-person.bin";
        InputStream isNF = new FileInputStream(modelPath);
        TokenNameFinderModel model = new TokenNameFinderModel(isNF);
        nameFinder = new NameFinderME(model);
    }

    private static List<String> parse(String text) throws IOException {
        List<String> words = new ArrayList<String>();
        String tokens[] = tokenizer.tokenize(text);

        //名字识别
        Span[] nameFinds = nameFinder.find(tokens);

        for (Span str : nameFinds) {
            words.add(tokens[str.getStart()]);
        }

        return words;
    }
```



**正则表达式标错**

```
java.lang.Exception: java.util.regex.PatternSyntaxException: Illegal repetition
{several sentences are missing here in the omnibus edition}
	at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:462)
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:522)
Caused by: java.util.regex.PatternSyntaxException: Illegal repetition
{several sentences are missing here in the omnibus edition}
	at java.util.regex.Pattern.error(Pattern.java:1969)
	at java.util.regex.Pattern.closure(Pattern.java:3171)
	at java.util.regex.Pattern.sequence(Pattern.java:2148)
	at java.util.regex.Pattern.expr(Pattern.java:2010)
	at java.util.regex.Pattern.compile(Pattern.java:1702)
	at java.util.regex.Pattern.<init>(Pattern.java:1352)
	at java.util.regex.Pattern.compile(Pattern.java:1028)
	at java.util.regex.Pattern.matches(Pattern.java:1133)
	at hadoop.CharacterImportantDegree$MapStep1.map(CharacterImportantDegree.java:82)
	at hadoop.CharacterImportantDegree$MapStep1.map(CharacterImportantDegree.java:67)
	at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:146)
	at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:787)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:341)
	at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:243)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
```



##### 1. **输出文件夹是空的？√**

- 反序列化失败了

  ![1664779020941](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664779020941.png)

<img src="C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664779481810.png" alt="1664779481810" style="zoom:50%;" />

write成功了，但是反序列化失败了。

https://blog.csdn.net/weixin_43230682/article/details/107916735

- 解决方案：

![1664783181219](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664783181219.png)



##### 2. hadoop 把一个文件分给一个mapper√

------------------ 不然，读不到书名和chapter名字

- 一个章节被分成了很多个map去执行，不是所有的数据都会被分给一个map

hadoop 把一个文件分给一个mapper

<img src="C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664781930812.png" alt="1664781930812" style="zoom: 67%;" />



**【同样的，在第二个job时，把几个整行分给一个mapper】**

------------------ 否则，不知道如何读入一个job的output，传到第二个job的mapper

> 
>
> **第二个Mapper读不了第一个Reducer的输出**
>
> LongWritable cannot be cast to org.[apache](https://so.csdn.net/so/search?q=apache&spm=1001.2101.3001.7020).hadoop.io.IntWritable
>
> 当写Map的时候，key的默认输入就是LongWritable。
>
> 因为LongWritable指代Block中的数据偏移量。
>
> org.apache.hadoop.io.Text cannot be cast to hadoop.appearTimeNeighbor
>
> value的默认输入就是Text 。



###### InputFormat

MR框架的数据源可以来自HDFS文件，也可以是例如查询数据库的输出等。文件的类型也没有规定特定的格式，例如也可以是网页。那么MR框架是如何读出不同类型的数据，然后形成键值对并作为Mapper（map()方法）的输入呢，这就是InputFormat的作用。不同的数据源，不同的数据格式，就需要采用不同的InputFormat类型，所以InputFormat是一个抽象类。

InputFormat描述了MR程序的输入规则，**其中getSplits()返回一个InputSplit的列表，一个分片对应一个Mapper。**如果给定Mapper的数量，那么分片的数量也就随之确认了。**但如果不给定Mapper的数量，如何进行分片就是不同类型的InputFormat需要考虑的问题了**。InputSplit是一个接口：

https://blog.csdn.net/qq_39192827/article/details/90228417

![1664804746173](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664804746173.png)



###### 重写一个inputFormat 实现一个mapper一个文件

https://blog.csdn.net/qq_43674360/article/details/109447995



###### 解决方案

```java
package hadoop.fileinputformat;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

;import java.io.IOException;

// 定义类继承FileInputFormat
public class WholeFileInputformat extends FileInputFormat<Text, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)	throws IOException, InterruptedException {

        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split, context);

        return recordReader;
    }
}

```

```java
package hadoop.fileinputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class WholeRecordReader extends RecordReader<Text, Text> {

    private Configuration configuration;
    private FileSplit split;

    private boolean isProgress= true;
    private Text value = new Text();
    private Text k = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.split = (FileSplit)split;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgress) {

            // 1 定义缓存区
            byte[] contents = new byte[(int)split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                // 2 获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);

                // 3 读取数据
                fis = fs.open(path);

                // 4 读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);

                // 5 输出文件内容
                value.set(contents, 0, contents.length);

// 6 获取文件路径及名称
                String name = split.getPath().toString();

// 7 设置输出的key值
                k.set(name);

            } catch (Exception e) {

            }finally {
                IOUtils.closeStream(fis);
            }

            isProgress = false;

            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}



```



##### 3. Text类型转string好奇怪√

###### 逆序列化后好奇怪

![1664786265279](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664786265279.png)



<img src="C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664802832652.png" alt="1664802832652" style="zoom:50%;" />



是否跟`java.io.EOFException: Premature EOF: no length prefix available`有关？



NLP的输出结果没问题

![1664791233282](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664791233282.png)

![1664791553624](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664791553624.png)



- 检测结果

`dataInput.readLine()`的结果是：`BOOK1`

奇怪的字符！



###### 解决方案

![1664809858165](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664809858165.png)



##### 4. 把感叹号/空格等等奇怪的东西当做人名√

加一个判断函数



##### 5. Neighbor没有去重√

解决方案: 利用`Set`数据结构

![1664814077529](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664814077529.png)



##### 6.Reducer没有把相同key的项合并？√

![1664824406030](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664824406030.png)

**1004 3：13**

job1的output没有合并同key的value；

但是job2合并得很好，尽管job1中的错误结果影响到了mean、std、range、neighbor





**（二）自定义key的合并规则**

方法：编写一个类继承WritableComparator，并重写compare方法

而且要写一个无参的构造器，调用super(key的类型,true)，不然可能会空指针异常
在通过job.setGroupingComparatorClass()设置自定义的合并规则





**Combiner合并**
(1) Combiner 是MR程序中Mapper和Reducer之外的一种组件。
(2) Combiner组件的父类就 是Reducer。
(3) Combiner和R educer的区别在于运行的位置
Combiner是在每一-个MapTask所在的节点运行;Reducer是接收全局所有Mapper的输出结果;
(4) Combiner的意义就是对每一个Map Task的输出进行局部汇总,以减小网络传输量。
(5) Combiner能够 应用的前提是不能影响最终的业务逻辑，而且，Combiner的输出kv应该跟R educer的输入kv类型要对应起来。

也就是说： Combiner是在map结束阶段对map的结果进行一次局部reduce，但是并不能改变map的输出结果。所以即使和真正的reduce阶段执行的任务相同也不能在Driver类中直接指定Combiner类为Reducer。除非map和reduce阶段的输入输出类型完全相同



###### 解决方案

**1004 1459**

不用key自定义的类，而用Text作为key



##### 7. 多本书的情况√

添加路径

![1664815051446](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664815051446.png)



##### 8. sum/mean/std/range有问题√

appear time好像一开始就有问题？

不知道一本书有多少chapter



###### 解决方案

在book名（文件名）上加上了chapter个数的信息



---

#### 1004 1633

跑了20本书，把一些明显不是人名的人名剔除了。



##### 判断节点重要性√

1. 以书为单位，人物为Node，构造图

2. page rank

https://graphstream-project.org/doc/Algorithms/PageRank/



##### **写第三个Mapper**√

##### debug：

- 空指针√
- 你奶奶的，迭代器只能用一次啊！√
  - copy之后，数量一样，内容变成一个了？√ 因为是**指向性的地址，地址都指到一个地方去了。**



###### 20books报错√

解决方案![1664894408795](C:\Users\61422\AppData\Roaming\Typora\typora-user-images\1664894408795.png)

----

### TODO

##### **择书（第三人称）**



##### **第三个 Job**√

- **邻居“图”分析**

  - 判断节点在图中的重要性

- **结果加权** 

  判断标准：

  1）sum越大越好；2）mean越大越好；3）std越小越好；4）range越小越好；5）在图中越中心越好
  $$
  30\times \text{RankOfNode} + 70\times[0.8\frac{\text{Mean}}{\text{Max_Mean}}+0.1(\frac{1}{std+1})+0.1(\frac{1}{\text{Range}+1})]
  $$



##### 第四个Job：TOP-k pattern √



##### **可视化**

- 以一本书为例
  - 评分过程可视化（图、sum/mean/std/range）
  - 最后的榜单（在数据分析时取top即可）
- 所有书的中心人物（第一名）展示









