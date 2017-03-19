package proj.mrs.combiners;

import proj.mrs.utils.FloatSumAndCount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class CombinerEvaluationPhaseThree extends
        Reducer<Text, FloatSumAndCount, Text, FloatSumAndCount> {

    @Override
    protected void reduce(Text key, Iterable<FloatSumAndCount> values,
            Context context) throws IOException, InterruptedException {
        int count = 0;
        float sum = 0.0f;
        for (FloatSumAndCount value : values) {
            sum += value.getSum().get();
            count += value.getCount().get();
        }
        context.write(key, new FloatSumAndCount(sum, count));
    }
}
