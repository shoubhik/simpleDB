package simpledb.query;

/**
 * User: shoubhik Date: 17/4/13 Time: 11:20 AM
 */
public class FieldAlias {

    private String original;
    private String alias;

    public FieldAlias(String original, String alias) {
        this.original = original;
        this.alias = alias;
    }

    public String getOriginal() {
        return original;
    }

    public String getAlias() {
        return alias;
    }
}
