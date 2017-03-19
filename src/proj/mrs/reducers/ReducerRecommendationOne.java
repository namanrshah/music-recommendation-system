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
public class ReducerRecommendationOne extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        List<CustomDataStructure> songAndPlayCount = null;
        List<String> candidateSongsList = null;
        List<CustomDataStructure> usersForWhomEmit = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.STARTS_WITH.USER_SONGS_PLAYCOUNT)) {
                valueString = valueString.substring(Constants.STARTS_WITH.USER_SONGS_PLAYCOUNT.length());
                String[] songAndCount = valueString.split(Constants.SEPARATORS.COMBO_SEPARATOR);
                int len = songAndCount.length;
                songAndPlayCount = new ArrayList<>(len);
                for (String songAndCount1 : songAndCount) {
                    String[] songPlayCount = songAndCount1.split(Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR);
                    CustomDataStructure cds = new CustomDataStructure(songPlayCount[0], songPlayCount[1]);
                    songAndPlayCount.add(cds);
                }
            } else if (valueString.startsWith(Constants.STARTS_WITH.CANDIDATE_SONGS_FOR_USER)) {
                valueString = valueString.substring(Constants.STARTS_WITH.CANDIDATE_SONGS_FOR_USER.length());
                String[] candidateSongs = valueString.split(Constants.SEPARATORS.COMBO_SEPARATOR);
                candidateSongsList = Arrays.asList(candidateSongs);
            } else {
                String[] userAndSimilarity = valueString.split(Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR);
                CustomDataStructure cds = new CustomDataStructure(userAndSimilarity[0], userAndSimilarity[1]);
                usersForWhomEmit.add(cds);
            }
        }
        if (usersForWhomEmit != null && !usersForWhomEmit.isEmpty() && songAndPlayCount != null && !songAndPlayCount.isEmpty()) {
            for (CustomDataStructure usersForWhomToEmit : usersForWhomEmit) {
                String user = usersForWhomToEmit.getProp1();
                Float similarity = Float.parseFloat(usersForWhomToEmit.getProp2());
                for (CustomDataStructure songPlayCount : songAndPlayCount) {
                    String song = songPlayCount.getProp1();
                    Float listenCount = Float.parseFloat(songPlayCount.getProp2());
                    context.write(new Text(user + Constants.SEPARATORS.COMBO_SEPARATOR + song), new Text(Float.toString(listenCount * similarity)));
                }
            }
        }
        //emitting candidate set
        if (candidateSongsList != null) {
            for (String candidateSongsList1 : candidateSongsList) {
                context.write(new Text(Constants.STARTS_WITH.CANDIDATE_SONGS_FOR_USER + key), new Text(candidateSongsList1));
            }
        }
    }
}
