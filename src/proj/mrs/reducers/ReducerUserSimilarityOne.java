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
public class ReducerUserSimilarityOne extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        StringBuffer strToWrite = new StringBuffer();
        List<UserAndPlayCount> userAndPlayCounts = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            String[] splitUserPlayCount = valueString.split(Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR);
            UserAndPlayCount upc = new UserAndPlayCount(splitUserPlayCount[0], Integer.parseInt(splitUserPlayCount[1]));
//            if (upc.getPlayCount() > 1) {
            userAndPlayCounts.add(upc);
//            }
        }
        if (userAndPlayCounts.size() > 1) {
            Collections.sort(userAndPlayCounts, new Comparator<UserAndPlayCount>() {

                @Override
                public int compare(UserAndPlayCount o1, UserAndPlayCount o2) {
                    return o1.getPlayCount() - o2.getPlayCount();
                }
            });
            for (UserAndPlayCount userAndPlayCount : userAndPlayCounts) {
                strToWrite.append(userAndPlayCount.getUserId() + Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR + userAndPlayCount.getPlayCount() + Constants.SEPARATORS.COMBO_SEPARATOR);
            }
            String str = strToWrite.toString().substring(0, strToWrite.length() - 1);
            context.write(key, new Text(str));
        }
    }
}
