package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.CustomDataStructure;
import proj.mrs.utils.UserAndPlayCount;
import proj.mrs.utils.UserAndSimilarity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerColdStartThree extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String users = key.toString();
        String[] splitUsers = users.split(Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR);
        float sum = 0;
        for (Text value : values) {
            String valueString = value.toString();
            sum += Float.parseFloat(valueString);
        }
        context.write(new Text(splitUsers[0]), new Text(splitUsers[1] + Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR + sum));
    }
}
