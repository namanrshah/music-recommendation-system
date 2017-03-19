package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.UserAndSimilarity;
import java.io.IOException;
import java.util.ArrayList;
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
public class ReducerUserSimilarityThree extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String user = key.toString();
        List<UserAndSimilarity> userAndSimilarity = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            String[] split = valueString.split(Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR);
            userAndSimilarity.add(new UserAndSimilarity(split[0], Float.parseFloat(split[1])));
        }
        Collections.sort(userAndSimilarity, (UserAndSimilarity o1, UserAndSimilarity o2) -> {
            if (o1.getSimilarity() < o2.getSimilarity()) {
                return 1;
            } else if (o2.getSimilarity() < o1.getSimilarity()) {
                return -1;
            } else {
                return 0;
            }
        });
        int size = userAndSimilarity.size();
        if (size > Constants.THRESHOLDS.SIMILAR_USERS) {
            size = Constants.THRESHOLDS.SIMILAR_USERS;
        }
        StringBuffer similarityString = new StringBuffer();
        for (int i = 0; i < size - 1; i++) {
            UserAndSimilarity userAndSim = userAndSimilarity.get(i);
            similarityString.append(userAndSim.getUserId() + Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR + userAndSim.getSimilarity() + Constants.SEPARATORS.COMBO_SEPARATOR);
        }
        similarityString.append(userAndSimilarity.get(size - 1).getUserId() + Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR + userAndSimilarity.get(size - 1).getSimilarity());
        context.write(new Text(Constants.STARTS_WITH.USER_SIMILARITY + key), new Text(similarityString.toString()));
    }
}
