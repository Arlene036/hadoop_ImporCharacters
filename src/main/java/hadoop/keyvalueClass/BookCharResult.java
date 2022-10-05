package hadoop.keyvalueClass;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BookCharResult implements Writable {
    private IntWritable sum;
    private DoubleWritable mean;
    private DoubleWritable std;
    private IntWritable range;
    private List<Text> neighbors;

    private IntWritable length;

    public IntWritable getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = new IntWritable(sum);
    }

    public DoubleWritable getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = new DoubleWritable(mean);
    }

    public DoubleWritable getStd() {
        return std;
    }

    public void setStd(double std) {
        this.std = new DoubleWritable(std);
    }

    public IntWritable getRange() {
        return range;
    }

    public void setRange(int range) {
        this.range = new IntWritable(range);
    }

    public List<Text> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<Text> neighbors) {
        this.neighbors = neighbors;
    }

    public void write(DataOutput dataOutput) throws IOException {
        sum.write(dataOutput);
        mean.write(dataOutput);
        std.write(dataOutput);
        range.write(dataOutput);

        length = new IntWritable(neighbors.size());
        length.write(dataOutput);

        for (Text neighbor : neighbors){
            neighbor.write(dataOutput);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        sum = new IntWritable();
        sum.readFields(dataInput);

        mean = new DoubleWritable();
        mean.readFields(dataInput);

        std = new DoubleWritable();
        std.readFields(dataInput);

        range = new IntWritable();
        range.readFields(dataInput);

        length = new IntWritable();
        length.readFields(dataInput);

        neighbors = new ArrayList<Text>();
        for(int i = 0; i < length.get() ; i++){
            Text t = new Text();
            t.readFields(dataInput);
            neighbors.add(t);
        }

    }

    public String toString(){
        StringBuffer sb = new StringBuffer();

        sb.append(sum.toString());
        sb.append("\t");

        sb.append(mean.toString());
        sb.append("\t");

        sb.append(std.toString());
        sb.append("\t");

        sb.append(range.toString());
        sb.append("\t");

        for (Text v : neighbors){
            sb.append(v.toString()+",");
        }
        return sb.toString();

    }
}
