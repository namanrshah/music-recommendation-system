package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.UserAndPlayCount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerUserSongsWithPlayCount extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        StringBuffer strToWrite = new StringBuffer();
        List<String> songs = new ArrayList<>();
        List<Integer> playCounts = new ArrayList<>();
        int sum = 0;
        for (Text value : values) {
//            strToWrite.append(value.toString() + Constants.SEPARATORS.COMBO_SEPARATOR);
            String valueString = value.toString();
            String[] songPlayCount = valueString.split(Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR);
            songs.add(songPlayCount[0]);
            int count = Integer.parseInt(songPlayCount[1]);
            playCounts.add(count);
            sum += count;
        }
        int len = playCounts.size();
        for (int i = 0; i < len; i++) {
            strToWrite.append(songs.get(i) + Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR + playCounts.get(i) * 1.0 / sum + Constants.SEPARATORS.COMBO_SEPARATOR);
        }
        context.write(new Text(Constants.STARTS_WITH.USER_SONGS_PLAYCOUNT + key), new Text(strToWrite.toString().substring(0, strToWrite.toString().length() - 1)));
    }
}
