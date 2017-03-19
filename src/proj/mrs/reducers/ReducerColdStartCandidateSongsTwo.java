package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

/**
 *
 * @author namanrs
 */
public class ReducerColdStartCandidateSongsTwo extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String user = key.toString();
        List<String> listenedSongsList = null;
        Set<String> candidateSongs = new HashSet<>();
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION)) {
                String listenedSongs = valueString.substring(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION.length());
                String[] listenedSongsArray = listenedSongs.split(Constants.SEPARATORS.COMBO_SEPARATOR);
                listenedSongsList = Arrays.asList(listenedSongsArray);
            } else {
                String[] candidateSongSubset = valueString.split(Constants.SEPARATORS.COMBO_SEPARATOR);
                int len = candidateSongSubset.length;
                for (int i = 0; i < len; i++) {
                    candidateSongs.add(candidateSongSubset[i]);
                }
            }
        }
        if (!candidateSongs.isEmpty() && listenedSongsList != null) {
            candidateSongs.removeAll(listenedSongsList);
            StringBuffer strToWrite = new StringBuffer();
            int size = candidateSongs.size();
            if (size > 0) {
                if (size > 100) {
                    size = 100;
                }
                List<String> candidateSongList = new ArrayList<>(candidateSongs);
                for (int i = 0; i < size; i++) {
                    strToWrite.append(candidateSongList.get(i) + Constants.SEPARATORS.COMBO_SEPARATOR);
                }
                context.write(new Text(Constants.STARTS_WITH.COLD_START_CANDIDATE_SONGS_FOR_USER + key), new Text(strToWrite.substring(0, strToWrite.length() - 1)));
            }
        }
    }
}
