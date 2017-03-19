package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.UserAndPlayCount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerSongToAvgValue extends
        Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
//        int sum = 0;
//        int count = 0;
//        for (IntWritable value : values) {
//            sum += value.get();
//            ++count;
//        }
//        context.write(key, new Text(Float.toString(sum * 1.0f / count)));
    }
}
