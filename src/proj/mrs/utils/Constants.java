package proj.mrs.utils;

import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author namanrs
 */
public class Constants {

    public static final IntWritable one = new IntWritable(1);

    public static class STARTS_WITH {

        public static final String USER_ID_USER_LISTENED_SONGS = "$U$";
        public static final String SONG_ID_SONGS_USER = "$S$";
        public static final String USER_SIMILARITY = "$US$";
        public static final String USER_SONGS_PLAYCOUNT = "$USP$";
        public static final String USER_SONGS = "$USN$";
        public static final String CANDIDATE_SONGS_FOR_USER = "$CS$";
        public static final String USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION = "$CUS$";
        public static final String RECO_FOR_COLD_START = "$RCS$";
        public static final String USERS_WITH_NO_RECO = "$UWNRS$";
        public static final String COLD_START_CANDIDATE_SONGS_FOR_USER = "$CSCS$";
    }

    public static class SEPARATORS {

        public static final String USER_PLAY_COUNT_SEPARATOR = "&%&";
        public static final String USER_SIMILARITY_SEPARATOR = "&@&";
        public static final String USER_SIMILARITY_USER_USER_SEPARATOR = "@&@";
        public static final String USER_SIMILARITY_COUNT_COUNT_SEPARATOR = "%&%";
        public static final String COMBO_SEPARATOR = ",";

        public static final String SONG_PLAY_COUNT_SEPARATOR = "@%@";
    }

    public static class THRESHOLDS {

        public static final int SIMILAR_USERS = 25;
        public static final int RECOMMENDATION_SIZE = 100;
        public static final int COLDSTART_ISSUE_NO_USER_TO_EMIT_FOR_SONG = 5;
        public static final float PERCENT_OF_SIMILARITY_FOR_EMIT = 0.75f;
    }

    public static class PATHS {

        public static String TRAINING_PATH_FOLDER = "/cs555/Term_Project/Input/Training_dataset_demo/";
//        public static String TRAINING_PATH_FOLDER = "/cs555/Term_Project/Input/Training_dataset/";
        public static String TRAINING_PATH_FILES = "training";
        public static String TESTING_PATH_FOLDER = "/cs555/Term_Project/Input/Testing_dataset_demo/";
//        public static String TESTING_PATH_FOLDER = "/cs555/Term_Project/Input/Testing_dataset/";
        public static String TESTING_PATH_FILES = "testing";
        public static String OUTPUT_PATH_FOR_TRAINING_TESTING_PARTITION_RUN = "/cs555/Term_Project/Output/Training/Output_demo/Output";
//        public static String OUTPUT_PATH_FOR_TRAINING_TESTING_PARTITION_RUN = "/cs555/Term_Project/Output/Training/Output";
    }
}
