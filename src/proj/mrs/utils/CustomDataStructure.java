package proj.mrs.utils;

/**
 *
 * @author namanrs
 */
public class CustomDataStructure {

    String prop1;
    String prop2;

    public CustomDataStructure() {
    }

    public CustomDataStructure(String userId, String playCount) {
        this.prop1 = userId;
        this.prop2 = playCount;
    }

    public String getProp1() {
        return prop1;
    }

    public void setProp1(String prop1) {
        this.prop1 = prop1;
    }

    public String getProp2() {
        return prop2;
    }

    public void setProp2(String prop2) {
        this.prop2 = prop2;
    }

    @Override
    public String toString() {
        return "UserAndPlayCount{" + "userId=" + prop1 + ", playCount=" + prop2 + '}';
    }
}
