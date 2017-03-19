package proj.mrs.mappers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.UserAndPlayCount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class MapperUserSimilarityTwo extends
        Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splitValue = valueString.split("\t");
//        String song = splitValue[0];
        String[] users = splitValue[1].split(Constants.SEPARATORS.COMBO_SEPARATOR);
        int userLen = users.length;

        List<String> userIds = new ArrayList<>(userLen);
        List<Integer> playCounts = new ArrayList<>(userLen);
        for (int i = 0; i < userLen; i++) {
            String[] userAndPlayCount = users[i].split(Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR);
            userIds.add(userAndPlayCount[0]);
            playCounts.add(Integer.parseInt(userAndPlayCount[1]));
        }
//        for (int i = 0; i < userLen; i++) {
//            for (int j = i + 1; j < userLen; j++) {
//                context.write(new Text(userIds.get(i) + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + userIds.get(j)), new Text(playCounts.get(i) + Constants.SEPARATORS.USER_SIMILARITY_COUNT_COUNT_SEPARATOR + playCounts.get(j)));
//            }
//        }
        if (userLen < 500) {
            for (int i = 0; i < userLen - 1; i++) {
                for (int j = i + 1; j < userLen; j++) {
                    int smaller = playCounts.get(i);
                    int bigger = playCounts.get(j);
                    //emit if smaller is >75% of bigger or diff is 1
                    if (smaller >= Constants.THRESHOLDS.PERCENT_OF_SIMILARITY_FOR_EMIT * bigger || bigger - smaller <= 1) {
                        //emit (c1 + c2 / abs(c1 - c2) + 1)
                        float similarity = (smaller + bigger) / ((bigger - smaller) + 1.0f);
                        int compareTo = userIds.get(i).compareTo(userIds.get(j));
                        if (compareTo < 0) {
                            context.write(new Text(userIds.get(i) + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + userIds.get(j)), new FloatWritable(similarity));
                        } else {
                            context.write(new Text(userIds.get(j) + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + userIds.get(i)), new FloatWritable(similarity));
                        }
                    } else {
                        break;
                    }
                }
            }
        } else {
            for (int i = 0; i < userLen - 1; i++) {
                for (int j = i + 1; j < userLen; j++) {
                    int smaller = playCounts.get(i);
                    int bigger = playCounts.get(j);
                    //emit if smaller is >75% of bigger or diff is 1
                    if (smaller == bigger) {
                        //emit (c1 + c2 / abs(c1 - c2) + 1)
                        float similarity = (smaller + bigger) / (1.0f);
                        int compareTo = userIds.get(i).compareTo(userIds.get(j));
                        if (compareTo < 0) {
                            context.write(new Text(userIds.get(i) + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + userIds.get(j)), new FloatWritable(similarity));
                        } else {
                            context.write(new Text(userIds.get(j) + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + userIds.get(i)), new FloatWritable(similarity));
                        }
                    } else {
                        break;
                    }
                }
            }

        }
    }
}
