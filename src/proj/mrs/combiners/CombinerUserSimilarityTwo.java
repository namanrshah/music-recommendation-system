package proj.mrs.combiners;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class CombinerUserSimilarityTwo extends
        Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values,
            Context context) throws IOException, InterruptedException {
        float count = 0;
        for (FloatWritable value : values) {
            count += value.get();
        }
        context.write(key, new FloatWritable(count));
    }
}
