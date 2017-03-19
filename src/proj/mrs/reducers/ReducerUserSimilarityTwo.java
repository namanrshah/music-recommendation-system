package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerUserSimilarityTwo extends
        Reducer<Text, FloatWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values,
            Context context) throws IOException, InterruptedException {
        String users = key.toString();
        String[] userSplits = users.split(Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR);
        float similarityCount = 0f;
        for (FloatWritable value : values) {
            similarityCount += value.get();
        }
        context.write(new Text(userSplits[0]), new Text(userSplits[1] + Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR + Float.toString(similarityCount)));
        context.write(new Text(userSplits[1]), new Text(userSplits[0] + Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR + Float.toString(similarityCount)));
    }
}
