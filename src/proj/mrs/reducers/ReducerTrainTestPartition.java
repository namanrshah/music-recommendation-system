package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.UserAndPlayCount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author namanrs
 */
public class ReducerTrainTestPartition extends
        Reducer<Text, Text, Text, Text> {

    MultipleOutputs out;

    public void setup(Context context) {
        out = new MultipleOutputs(context);

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
//        int count = 0;
//        List<Integer> vals = new ArrayList<>();
        List<String> valueList = new ArrayList<>();
        for (Text value : values) {
//            ++count;
//            vals.add(count);
            valueList.add(value.toString());
        }
        Collections.shuffle(valueList);
        int len = valueList.size();
//        context.write(key, new Text(Integer.toString(len)));
        int limit = (int) (0.8 * len);
        for (int i = 0; i < len; i++) {
            if (i < limit) {
                out.write(key, valueList.get(i), "hdfs://" + Constants.PATHS.TRAINING_PATH_FOLDER +Constants.PATHS.TRAINING_PATH_FILES);
//            context.write(key, new Text(valueList.get(i)));
            } else {
                out.write(key, valueList.get(i), "hdfs://" + Constants.PATHS.TESTING_PATH_FOLDER + Constants.PATHS.TESTING_PATH_FILES);
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
    }
}
