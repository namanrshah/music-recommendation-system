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
public class MapperEvaluationPhaseOne extends
        Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        String userId = splittedValues[0];
        if (splittedValues[1].contains(Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR)) {
            //from reco
            String recommendations = splittedValues[1];
            String[] songsAndSim = recommendations.split(Constants.SEPARATORS.COMBO_SEPARATOR);
            int len = songsAndSim.length;
            String strToWrite = "";
            for (int i = 0; i < len - 1; i++) {
                String[] split = songsAndSim[i].split(Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR);
                strToWrite += split[0] + Constants.SEPARATORS.COMBO_SEPARATOR;
            }
            strToWrite += songsAndSim[len - 1].split(Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR)[0];
            context.write(new Text(userId), new Text(strToWrite));
        } else {
            //from nonreco
            userId = userId.substring(Constants.STARTS_WITH.COLD_START_CANDIDATE_SONGS_FOR_USER.length());
            context.write(new Text(userId), new Text(splittedValues[1]));
        }
    }
}
