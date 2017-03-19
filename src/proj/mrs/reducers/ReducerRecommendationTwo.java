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
public class ReducerRecommendationTwo extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        boolean validCandidateSong = false;
        float sum = 0.0f;
        int count = 0;
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.STARTS_WITH.CANDIDATE_SONGS_FOR_USER)) {
                validCandidateSong = true;
            } else {
                sum += Float.parseFloat(value.toString());
                ++count;
            }
        }
        if (validCandidateSong) {
            String[] userSong = key.toString().split(Constants.SEPARATORS.COMBO_SEPARATOR);
            context.write(new Text(userSong[0]), new Text(userSong[1] + Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR + Float.toString(sum / count)));
        }
    }
}
