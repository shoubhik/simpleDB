package simpledb.query;

import simpledb.tx.Transaction;

/**
 * User: shoubhik Date: 17/4/13 Time: 12:33 PM
 */
public class AliasTablePlanDecorator extends TablePlan{

    private FieldAliasCollection fieldAliasCollection;

    public AliasTablePlanDecorator (String tblname, Transaction tx,
                                    FieldAliasCollection fieldAliasCollection) {
        super(tblname, tx);
        this.fieldAliasCollection = fieldAliasCollection;

    }

    public Scan open() {
        return new AliasTableScanDecorator(this.fieldAliasCollection ,
                                           (TableScan)super.open());
    }

    public String toString(){
        return super.toString();
    }
}
