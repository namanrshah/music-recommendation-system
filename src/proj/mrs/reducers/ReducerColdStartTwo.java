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
public class ReducerColdStartTwo extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        boolean nonRecoUserFound = false;
        List<UserAndPlayCount> nonRecoUsersAndPlayCounts = new ArrayList<>();
        List<UserAndPlayCount> recoUsersAndPlayCount = new ArrayList<>();
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.STARTS_WITH.USERS_WITH_NO_RECO)) {
                valueString = valueString.substring(Constants.STARTS_WITH.USERS_WITH_NO_RECO.length());
                String[] nonRecoUP = valueString.split(Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR);
                UserAndPlayCount nonUP = new UserAndPlayCount(nonRecoUP[0], Integer.parseInt(nonRecoUP[1]));
                nonRecoUsersAndPlayCounts.add(nonUP);
                nonRecoUserFound = true;
            } else {
                String[] allUsers = valueString.split(Constants.SEPARATORS.COMBO_SEPARATOR);
                int len = allUsers.length;
                recoUsersAndPlayCount = new ArrayList<>(len);
                for (int i = 0; i < len; i++) {
                    String allU = allUsers[i];
                    String[] userAndCount = allU.split(Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR);
                    UserAndPlayCount recoUP = new UserAndPlayCount(userAndCount[0], Integer.parseInt(userAndCount[1]));
                    recoUsersAndPlayCount.add(recoUP);
                }
            }
        }
        if (nonRecoUserFound) {
            if (!recoUsersAndPlayCount.isEmpty()) {
                Collections.shuffle(recoUsersAndPlayCount);
                int nonRecoLen = nonRecoUsersAndPlayCounts.size();
                int recoLen = recoUsersAndPlayCount.size();
                if (recoLen >= Constants.THRESHOLDS.COLDSTART_ISSUE_NO_USER_TO_EMIT_FOR_SONG) {
                    for (int i = 0; i < nonRecoLen; i++) {
                        for (int j = 0; j < Constants.THRESHOLDS.COLDSTART_ISSUE_NO_USER_TO_EMIT_FOR_SONG; j++) {
                            int index = (Constants.THRESHOLDS.COLDSTART_ISSUE_NO_USER_TO_EMIT_FOR_SONG * i + j) % recoLen;
                            String nonRecoUserId = nonRecoUsersAndPlayCounts.get(i).getUserId();
                            int nonRecoUserCount = nonRecoUsersAndPlayCounts.get(i).getPlayCount();
                            String recoUserId = recoUsersAndPlayCount.get(index).getUserId();
                            int recoUserCount = recoUsersAndPlayCount.get(index).getPlayCount();
                            if (!nonRecoUserId.equals(recoUserId)) {
                                context.write(new Text(nonRecoUserId + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + recoUserId), new Text(Float.toString((nonRecoUserCount + recoUserCount) * 1.0f / (Math.abs(nonRecoUserCount - recoUserCount) + 1))));
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < nonRecoLen; i++) {
                        for (int j = 0; j < recoLen; j++) {
//                            int index = (Constants.THRESHOLDS.COLDSTART_ISSUE_NO_USER_TO_EMIT_FOR_SONG * i + j) % recoLen;
                            String nonRecoUserId = nonRecoUsersAndPlayCounts.get(i).getUserId();
                            int nonRecoUserCount = nonRecoUsersAndPlayCounts.get(i).getPlayCount();
                            String recoUserId = recoUsersAndPlayCount.get(j).getUserId();
                            int recoUserCount = recoUsersAndPlayCount.get(j).getPlayCount();
                            if (!nonRecoUserId.equals(recoUserId)) {
                                context.write(new Text(nonRecoUserId + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + recoUserId), new Text(Float.toString((nonRecoUserCount + recoUserCount) * 1.0f / (Math.abs(nonRecoUserCount - recoUserCount) + 1))));
                            }
                        }
                    }
                }
            } else {
                //No one has listed this song from users who got recommendations, so compare similarity between users facing cold start issue
                int nonRecoLen = nonRecoUsersAndPlayCounts.size();
                if (nonRecoLen > 1) {
                    int innerLoopCount = nonRecoLen;
                    if (nonRecoLen > 5) {
                        innerLoopCount = 5;
                    }
                    for (int i = 0; i < nonRecoLen; i++) {
                        for (int j = 0; j < innerLoopCount; j++) {
                            if (i != j) {
                                String nonRecoUserId = nonRecoUsersAndPlayCounts.get(i).getUserId();
                                int nonRecoUserCount = nonRecoUsersAndPlayCounts.get(i).getPlayCount();
                                String recoUserId = nonRecoUsersAndPlayCounts.get(j).getUserId();
                                int recoUserCount = nonRecoUsersAndPlayCounts.get(j).getPlayCount();
                                context.write(new Text(nonRecoUserId + Constants.SEPARATORS.USER_SIMILARITY_USER_USER_SEPARATOR + recoUserId), new Text(Float.toString((nonRecoUserCount + recoUserCount) * 1.0f / (Math.abs(nonRecoUserCount - recoUserCount) + 1))));
                            }
                        }
                    }
                } else {
                    //only one user has listened to given song, so recommend best songs to it.
                }
            }
        }
    }
}
