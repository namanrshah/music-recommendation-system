package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerEvaluationPhaseOne extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String users = key.toString();
        Set<String> recommendations = new HashSet<>();
        for (Text value : values) {
            String[] songs = value.toString().split(Constants.SEPARATORS.COMBO_SEPARATOR);
            recommendations.addAll(Arrays.asList(songs));
        }
        if (!recommendations.isEmpty()) {
            int len = recommendations.size();
            StringBuffer strToWrite = new StringBuffer();
            for (String recommendation : recommendations) {
                strToWrite.append(recommendation + Constants.SEPARATORS.COMBO_SEPARATOR);
            }
            context.write(new Text(users), new Text(strToWrite.substring(0, strToWrite.length() - 1)));
        }
    }
}
