package simpledb.query;

import simpledb.record.RID;

/**
 * User: shoubhik Date: 17/4/13 Time: 12:25 PM
 */
public class AliasTableScanDecorator extends TableScan{

    private FieldAliasCollection fieldAliasCollection;
    private TableScan tableScan;


    public AliasTableScanDecorator(FieldAliasCollection fieldAliasCollection,
                                   TableScan tableScan){
        this.fieldAliasCollection = fieldAliasCollection;
        this.tableScan = tableScan;
    }


    // Scan methods

    public void beforeFirst() {
        this.tableScan.beforeFirst();
    }

    public boolean next() {
        return this.tableScan.next();
    }

    public void close() {
        this.tableScan.close();
    }

    /**
     * Returns the value of the specified field, as a Constant.
     * The schema is examined to determine the field's type.
     * If INTEGER, then the record file's getInt method is called;
     * otherwise, the getString method is called.
     * @see simpledb.query.Scan#getVal(String)
     */
    public Constant getVal(String fldname) {
        return this.tableScan.getVal(this.fieldAliasCollection.getOriginalName(
                fldname));
    }

    public int getInt(String fldname) {
        return this.tableScan.getInt(this.fieldAliasCollection.getOriginalName(
                fldname));
    }

    public String getString(String fldname) {
        return this.tableScan.getString(this.fieldAliasCollection.getOriginalName(
                fldname));
    }

    public boolean hasField(String fldname) {
        return this.tableScan.hasField(this.fieldAliasCollection.getOriginalName(
                fldname));
    }

    // UpdateScan methods

    /**
     * Sets the value of the specified field, as a Constant.
     * The schema is examined to determine the field's type.
     * If INTEGER, then the record file's setInt method is called;
     * otherwise, the setString method is called.
     * @see UpdateScan#setVal(String, Constant)
     */
    public void setVal(String fldname, Constant val) {
        this.tableScan.setVal(fldname, val);
    }

    public void setInt(String fldname, int val) {
        this.tableScan.setInt(fldname, val);
    }

    public void setString(String fldname, String val) {
        this.tableScan.setString(fldname, val);
    }

    public void delete() {
        this.tableScan.delete();
    }

    public void insert() {
        this.tableScan.insert();
    }

    public RID getRid() {
        return this.tableScan.getRid();
    }

    public void moveToRid(RID rid) {
        this.tableScan.moveToRid(rid);
    }

    @Override
    public boolean isNull(String fldName) {
        return this.tableScan.isNull(this.fieldAliasCollection.getOriginalName(
                fldName));
    }



}
