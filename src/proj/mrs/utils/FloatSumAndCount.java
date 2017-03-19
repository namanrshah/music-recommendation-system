package proj.mrs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author namanrs
 */
public class FloatSumAndCount implements Writable {

    FloatWritable sum = new FloatWritable();
    IntWritable count = new IntWritable();

    public FloatSumAndCount() {
        this.sum = new FloatWritable();
        this.count = new IntWritable();
    }

    public FloatSumAndCount(float ratingIntoHelpfulness, int count) {
        this.sum.set(ratingIntoHelpfulness);
        this.count.set(count);
    }

    public FloatWritable getSum() {
        return sum;
    }

    public void setSum(FloatWritable sum) {
        this.sum = sum;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        sum.write(d);
        count.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        sum.readFields(di);
        count.readFields(di);
    }
}
