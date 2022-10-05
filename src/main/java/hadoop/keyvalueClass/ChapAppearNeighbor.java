package hadoop.keyvalueClass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChapAppearNeighbor implements WritableComparable<ChapAppearNeighbor> {
    private IntWritable value_appear;
    private List<Text> neighbors;

    private IntWritable length;


    public IntWritable getValue_appear() {
        return value_appear;
    }

    public void setValue_appear(IntWritable value_appear) {
        this.value_appear = value_appear;
    }

    public List<Text> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<Text> neighbors) {
        this.neighbors = neighbors;
    }

    public void setNeighbors(String[] neighbors){
        List<Text> temp = new ArrayList<Text>();
        for(String nb : neighbors){
            temp.add(new Text(nb));
        }
        this.neighbors = temp;
    }

    public int compareTo(ChapAppearNeighbor o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        value_appear.write(dataOutput);

        length = new IntWritable(neighbors.size());
        length.write(dataOutput);

        for (Text t:neighbors){
            t.write(dataOutput);
        }

    }

    public void readFields(DataInput dataInput) throws IOException {
        value_appear = new IntWritable();
        value_appear.readFields(dataInput);

        length = new IntWritable();
        length.readFields(dataInput);

        neighbors = new ArrayList<Text>();
        for(int i = 0; i < length.get() ; i++){
            Text t = new Text();
            t.readFields(dataInput);
            neighbors.add(t);
        }
    }
}
