package proj.mrs.reducers;

import proj.mrs.utils.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class ReducerUserToSongsListened extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        List<Integer> counts = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            String[] split = valueString.split(Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR);
            counts.add(Integer.parseInt(split[1]));
        }
        Collections.sort(counts);
        context.write(new Text(key), new Text(counts.size() + counts.toString()));
    }
}
