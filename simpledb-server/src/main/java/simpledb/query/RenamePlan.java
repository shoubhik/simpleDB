package simpledb.query;

import simpledb.record.AliasSchemaDecorator;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * User: shoubhik Date: 19/4/13 Time: 6:29 PM
 */
public class RenamePlan implements Plan{

    private AliasCollection aliasCollection;
    private TablePlan tablePlan;
    private String tblName;

    public RenamePlan(String tblname, Transaction tx, String original, String renameTo){
        this.aliasCollection = new AliasCollection();
        this.aliasCollection.addAlias(original, renameTo);
        this.tablePlan = new TablePlan(tblname, tx);
        this.tblName = tblname;

    }

    @Override
    public Scan open() {
        return new RenameScan(tablePlan.open(), aliasCollection);
    }

    @Override
    public int blocksAccessed() {
        return this.tablePlan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return this.tablePlan.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return this.tablePlan.distinctValues(this.aliasCollection.getOriginal(fldname));
    }

    @Override
    public Schema schema() {
        return new AliasSchemaDecorator(this.aliasCollection, this.tablePlan.schema());
    }

    public String toString(){
        return "rename (" +  tblName + ")" + ", {"  + aliasCollection.toString() + " }";
    }
}
