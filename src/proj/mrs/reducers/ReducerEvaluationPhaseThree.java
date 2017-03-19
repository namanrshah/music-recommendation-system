package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.FloatSumAndCount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerEvaluationPhaseThree extends
        Reducer<Text, FloatSumAndCount, NullWritable, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatSumAndCount> values,
            Context context) throws IOException, InterruptedException {
        float sum = 0.0f;
        int count = 0;
        for (FloatSumAndCount value : values) {
            sum += value.getSum().get();
            count += value.getCount().get();
        }
        context.write(NullWritable.get(), new FloatWritable(sum / count));
    }
}
