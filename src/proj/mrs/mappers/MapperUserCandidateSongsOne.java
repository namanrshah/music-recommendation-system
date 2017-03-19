package proj.mrs.mappers;

import proj.mrs.utils.Constants;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class MapperUserCandidateSongsOne extends
        Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        String userId = splittedValues[0];
        if (userId.startsWith(Constants.STARTS_WITH.USER_SIMILARITY)) {//user-similarity datatset
            userId = userId.substring(Constants.STARTS_WITH.USER_SIMILARITY.length());
            String[] usersWithSimilarity = splittedValues[1].split(Constants.SEPARATORS.COMBO_SEPARATOR);
            int len = usersWithSimilarity.length;
            for (int i = 0; i < len; i++) {
                String user = usersWithSimilarity[i];
                String[] userAnSimilarity = user.split(Constants.SEPARATORS.USER_SIMILARITY_SEPARATOR);
                context.write(new Text(userAnSimilarity[0]), new Text(userId));
            }
        } else {//User-songs set
            userId = userId.substring(Constants.STARTS_WITH.USER_SONGS.length());
            context.write(new Text(userId), new Text(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION + splittedValues[1]));
        }
    }
}
