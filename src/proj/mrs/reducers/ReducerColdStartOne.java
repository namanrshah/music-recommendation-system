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
public class ReducerColdStartOne extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        boolean recoFound = false;
        List<CustomDataStructure> songsAndPlayCounts = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.equals(Constants.STARTS_WITH.RECO_FOR_COLD_START)) {
                recoFound = true;
            } else {
                String[] songPlayCountSplit = valueString.split(Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR);
                CustomDataStructure csd = new CustomDataStructure(songPlayCountSplit[0], songPlayCountSplit[1]);
                songsAndPlayCounts.add(csd);
            }
        }
        if (!recoFound) {
            if (!songsAndPlayCounts.isEmpty()) {
                for (CustomDataStructure songsAndPlayCount : songsAndPlayCounts) {
                    context.write(new Text(Constants.STARTS_WITH.USERS_WITH_NO_RECO + key), new Text(songsAndPlayCount.getProp1() + "\t" + songsAndPlayCount.getProp2()));
                }
            }
        }
    }
}
