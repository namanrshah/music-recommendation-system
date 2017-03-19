package proj.mrs.utils;

/**
 *
 * @author namanrs
 */
public class UserAndSimilarity {

    String userId;
    float similarity;

    public UserAndSimilarity() {
    }

    public UserAndSimilarity(String userId, float playCount) {
        this.userId = userId;
        this.similarity = playCount;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public float getSimilarity() {
        return similarity;
    }

    public void setSimilarity(float similarity) {
        this.similarity = similarity;
    }

    @Override
    public String toString() {
        return "UserAndPlayCount{" + "userId=" + userId + ", similarity=" + similarity + '}';
    }
}
