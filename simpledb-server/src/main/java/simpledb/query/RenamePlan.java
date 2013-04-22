package simpledb.query;

import simpledb.record.AliasSchemaDecorator;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * User: shoubhik Date: 19/4/13 Time: 6:29 PM
 */
public class RenamePlan implements Plan{

    private AliasCollection aliasCollection;
    private Plan p;

    public RenamePlan(Plan plan, String original, String renameTo){
        this.aliasCollection = new AliasCollection();
        this.aliasCollection.addAlias(original, renameTo);
        this.p = plan;

    }

    @Override
    public Scan open() {
        return new RenameScan(p.open(), aliasCollection);
    }

    @Override
    public int blocksAccessed() {
        return this.p.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return this.p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return this.p.distinctValues(this.aliasCollection.getOriginal(fldname));
    }

    @Override
    public Schema schema() {
        return new AliasSchemaDecorator(this.aliasCollection, this.p.schema());
    }

    public String toString(){
        return "rename (" +  p.toString() + ")" + ", {"  + aliasCollection.toString() + " }";
    }
}
