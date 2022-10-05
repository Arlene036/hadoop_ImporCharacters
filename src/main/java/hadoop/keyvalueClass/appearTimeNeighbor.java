package hadoop.keyvalueClass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class appearTimeNeighbor implements WritableComparable<appearTimeNeighbor> {
    IntWritable appear_times;
    List<Text> value;
    private IntWritable length;

    public appearTimeNeighbor(Text value) {
        String[] vs = value.toString().split(",");
    }


    public IntWritable getAppear_times() {
        return appear_times;
    }

    public void setAppear_times(int appear_times) {
        this.appear_times = new IntWritable(appear_times);
    }

    public void setValue(List<Text> value) {
        this.value = value;
    }

    public List<Text> getValue() {
        return value;
    }

    public appearTimeNeighbor(int appear_times, List<Text> value){
        this.appear_times = new IntWritable(appear_times);
        this.value = value;
    }

    public appearTimeNeighbor(){
        this.appear_times = new IntWritable(0);
        this.value = new ArrayList<Text>();
    }

    public void write(DataOutput dataOutput) throws IOException {
        appear_times.write(dataOutput);

        length = new IntWritable(value.size());
        length.write(dataOutput);

        for (Text t:value){
            new Text(t).write(dataOutput);
        }
//        new Text(",").write(dataOutput);
//        for (Text t:value){
//            new Text(t+",").write(dataOutput);
//        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        appear_times = new IntWritable();
        appear_times.readFields(dataInput);


        length = new IntWritable();
        length.readFields(dataInput);

        value = new ArrayList<Text>();
        for(int i = 0; i < length.get() ; i++){
            Text t = new Text();
            t.readFields(dataInput);
            value.add(t);
        }



//        this.appear_times = new IntWritable(dataInput.readInt());
//
//        String nb = dataInput.readLine(); //nb = ",This,..."
//        String[] nbs = nb.split(",");
//        List<Text> nbList = new ArrayList<Text>();
//        for (String neighbor : nbs){
//            nbList.add(new Text(neighbor));
//        }
//        this.value = nbList;
    }


    //no need to rewrite this
    public int compareTo(appearTimeNeighbor o) {
        return 0;
    }

    public void add (appearTimeNeighbor o){
        this.appear_times = new IntWritable(this.appear_times.get()+  o.appear_times.get());

        //去重（求并集）
        this.value.removeAll(o.value);
        this.value.addAll(o.value);
    }


    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append(appear_times.toString());
        sb.append("\t");
        for (Text v : value){
            sb.append(v.toString()+",");
        }
        return sb.toString();
    }
}
