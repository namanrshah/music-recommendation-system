package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerEvaluationPhaseTwo extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String users = key.toString();
        List<String> songsFromTest = new ArrayList<>();
        List<String> songsFromReco = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.STARTS_WITH.USER_SONGS)) {
                valueString = valueString.substring(Constants.STARTS_WITH.USER_SONGS.length());
                songsFromTest.add(valueString);
            } else {
                String[] recoSongs = valueString.split(Constants.SEPARATORS.COMBO_SEPARATOR);
                songsFromReco = Arrays.asList(recoSongs);
            }
        }
        if (!songsFromReco.isEmpty() && !songsFromTest.isEmpty()) {
            int foundCount = 0;
            int totCount = songsFromTest.size();
            for (String songsFromTest1 : songsFromTest) {
                if (songsFromReco.contains(songsFromTest1)) {
                    ++foundCount;
                }
            }
            context.write(new Text(users), new Text(Float.toString(foundCount * 100f / totCount)));
        }
    }
}
