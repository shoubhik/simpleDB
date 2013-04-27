package simpledb.query;

/**
 * User: shoubhik Date: 19/4/13 Time: 7:49 PM
 */
public class NullConstant implements Constant{
    @Override
    public String asJavaVal() {
        return "null";
    }

    @Override
    public int compareTo(Constant o) {
        return o instanceof NullConstant ? 0 : 1;
    }

    public boolean equals(Object o){
        return o instanceof NullConstant;
    }

    public String toString() {
        return asJavaVal();
    }
}
