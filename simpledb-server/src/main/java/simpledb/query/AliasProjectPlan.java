package simpledb.query;

import simpledb.record.AliasSchemaDecorator;
import simpledb.record.Schema;

import java.util.Collection;

/**
 * User: shoubhik Date: 17/4/13 Time: 7:15 PM
 */
public class AliasProjectPlan implements Plan {

    private Plan p;
    private Schema schema ;

    /**
     * Creates a new project node in the query tree,
     * having the specified subquery and field list.
     * @param p the subquery
     */
    public AliasProjectPlan(Plan p, FieldAliasCollection fieldAliasCollection) {
        this.p = p;
        this.schema = new AliasSchemaDecorator(fieldAliasCollection, new Schema());
        for (String fldname : fieldAliasCollection.getAliasFields())
            schema.add(fldname, p.schema());
    }

    /**
     * Creates a project scan for this query.
     * @see simpledb.query.Plan#open()
     */
    public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, schema.fields());
    }

    /**
     * Estimates the number of block accesses in the projection,
     * which is the same as in the underlying query.
     * @see simpledb.query.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    /**
     * Estimates the number of output records in the projection,
     * which is the same as in the underlying query.
     * @see simpledb.query.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Estimates the number of distinct field values
     * in the projection,
     * which is the same as in the underlying query.
     * @see simpledb.query.Plan#distinctValues(String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the projection,
     * which is taken from the field list.
     * @see simpledb.query.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }

    public String toString(){
        return "project ( " + p.toString() + ", { "  + schema().toString() + " }) ";
    }

}
