package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.UserAndSimilarity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerColdStartanalysis extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
//        String user = key.toString();
        String songs = "";
//        List<String> users = new ArrayList<>();
        boolean usersimilarityFound = false;
        boolean candidateSongsFoune = false;
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION)) {
                songs = valueString.substring(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION.length());
                candidateSongsFoune = true;
            } else if (valueString.startsWith(Constants.STARTS_WITH.USER_SIMILARITY)) {
//                users.add(valueString);
                usersimilarityFound = true;
            }
        }
        if (!usersimilarityFound && candidateSongsFoune) {
            context.write(key, new Text(Integer.toString(songs.split(Constants.SEPARATORS.COMBO_SEPARATOR).length)));
        }
//        if (!songs.isEmpty()) {
//            for (String user1 : users) {
//                context.write(new Text(user1), new Text(songs));
//            }
//        }
    }
}
