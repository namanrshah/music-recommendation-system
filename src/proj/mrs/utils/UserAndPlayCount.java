package proj.mrs.utils;

/**
 *
 * @author namanrs
 */
public class UserAndPlayCount {

    String userId;
    int playCount;

    public UserAndPlayCount() {
    }

    public UserAndPlayCount(String userId, int playCount) {
        this.userId = userId;
        this.playCount = playCount;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getPlayCount() {
        return playCount;
    }

    public void setPlayCount(int playCount) {
        this.playCount = playCount;
    }

    @Override
    public String toString() {
        return "UserAndPlayCount{" + "userId=" + userId + ", playCount=" + playCount + '}';
    }
}
