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
public class ReducerRecommendationThree extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        List<UserAndSimilarity> songAndValues = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            String[] songAndVal = valueString.split(Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR);
            songAndValues.add(new UserAndSimilarity(songAndVal[0], Float.parseFloat(songAndVal[1])));
        }
        Collections.sort(songAndValues, (UserAndSimilarity o1, UserAndSimilarity o2) -> {
            if (o1.getSimilarity() < o2.getSimilarity()) {
                return 1;
            } else if (o2.getSimilarity() < o1.getSimilarity()) {
                return -1;
            } else {
                return 0;
            }
        });
        int size = songAndValues.size();
        if (size > Constants.THRESHOLDS.RECOMMENDATION_SIZE) {
            size = Constants.THRESHOLDS.RECOMMENDATION_SIZE;
        }
        StringBuffer strToWrite = new StringBuffer();
        for (int i = 0; i < size - 1; i++) {
            strToWrite.append(songAndValues.get(i).getUserId() + Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR + songAndValues.get(i).getSimilarity() + Constants.SEPARATORS.COMBO_SEPARATOR);
        }
        strToWrite.append(songAndValues.get(size - 1).getUserId() + Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR + songAndValues.get(size - 1).getSimilarity());
        context.write(key, new Text(strToWrite.toString()));
    }
}
