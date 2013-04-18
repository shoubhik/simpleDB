package simpledb.query;

/**
 * User: shoubhik Date: 17/4/13 Time: 9:56 PM
 */
public class NullConstant implements Constant {
    @Override
    public String asJavaVal() {
        return "null";
    }

    @Override
    public int compareTo(Constant o) {
        return o instanceof NullConstant ? 0 : 1;
    }
}
