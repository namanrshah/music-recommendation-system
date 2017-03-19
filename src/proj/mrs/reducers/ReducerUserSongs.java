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
public class ReducerUserSongs extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        StringBuffer strToWrite = new StringBuffer();
        for (Text value : values) {
            strToWrite.append(value.toString() + Constants.SEPARATORS.COMBO_SEPARATOR);
        }
        context.write(new Text(Constants.STARTS_WITH.USER_SONGS + key), new Text(strToWrite.toString().substring(0, strToWrite.toString().length() - 1)));
    }
}
