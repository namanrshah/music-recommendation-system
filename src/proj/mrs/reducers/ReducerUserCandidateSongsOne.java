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
public class ReducerUserCandidateSongsOne extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String user = key.toString();
        String songs = "";
        List<String> users = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION)) {
                songs = valueString.substring(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION.length());
            } else {
                users.add(valueString);
            }
        }
        if (!songs.isEmpty()) {
            for (String user1 : users) {
                context.write(new Text(user1), new Text(songs));
            }
        }
    }
}
