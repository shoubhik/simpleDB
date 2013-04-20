package simpledb.query;

/**
 * User: shoubhik Date: 19/4/13 Time: 7:05 PM
 */
public class RenameScan implements Scan {

    private Scan s;
    private AliasCollection aliasCollection;

    public RenameScan(Scan s, AliasCollection aliasCollection){
        this.s = s;
        this.aliasCollection = aliasCollection;

    }
    @Override
    public void beforeFirst() {
        this.s.beforeFirst();
    }

    @Override
    public boolean next() {
        return this.s.next();
    }

    @Override
    public void close() {
        this.s.close();
    }

    @Override
    public Constant getVal(String fldname) {
        return this.s.getVal(aliasCollection.getOriginal(fldname));
    }

    @Override
    public int getInt(String fldname) {
        return this.s.getInt(aliasCollection.getOriginal(fldname));
    }

    @Override
    public String getString(String fldname) {
        return this.s.getString(aliasCollection.getOriginal(fldname));
    }

    @Override
    public boolean hasField(String fldname) {
        return this.s.hasField(aliasCollection.getOriginal(fldname));
    }
}
