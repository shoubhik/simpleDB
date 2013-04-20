package simpledb.record;

import simpledb.query.AliasCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * User: shoubhik Date: 19/4/13 Time: 6:51 PM
 */
public class AliasSchemaDecorator extends Schema{

    private AliasCollection aliasCollection;
    private Schema schema;

    public AliasSchemaDecorator(AliasCollection aliasCollection, Schema schema){
        this.aliasCollection = aliasCollection;
        this.schema = schema;
    }

    /**
     * Adds a field to the schema having a specified
     * name, type, and length.
     * If the field type is "integer", then the length
     * value is irrelevant.
     * @param fldname the name of the field
     * @param type the type of the field, according to the constants in simpledb.sql.types
     * @param length the conceptual length of a string field.
     */
    public void addField(String fldname, int type, int length) {
        this.schema.addField(getName(fldname), type, length);

    }

    /**
     * Adds an integer field to the schema.
     * @param fldname the name of the field
     */
    public void addIntField(String fldname) {
        this.schema.addIntField(getName(fldname));
    }

    /**
     * Adds a string field to the schema.
     * The length is the conceptual length of the field.
     * For example, if the field is defined as varchar(8),
     * then its length is 8.
     * @param fldname the name of the field
     * @param length the number of chars in the varchar definition
     */
    public void addStringField(String fldname, int length) {
        this.schema.addStringField(getName(fldname), length);
    }

    /**
     * Adds a field to the schema having the same
     * type and length as the corresponding field
     * in another schema.
     * @param fldname the name of the field
     * @param sch the other schema
     */
    public void add(String fldname, Schema sch) {
        this.add(getName(fldname), sch);
    }

    /**
     * Adds all of the fields in the specified schema
     * to the current schema.
     * @param sch the other schema
     */
    public void addAll(Schema sch) {
        this.schema.addAll(sch);
    }

    /**
     * Returns a collection containing the name of
     * each field in the schema.
     * @return the collection of the schema's field names
     */
    public Collection<String> fields() {
        Collection<String> aliases = new ArrayList<String>();
        for(Iterator<String> i = this.schema.fields().iterator(); i.hasNext();)
            aliases.add(aliasCollection.getAlias(i.next()));
        return aliases;
    }

    /**
     * Returns true if the specified field
     * is in the schema
     * @param fldname the name of the field
     * @return true if the field is in the schema
     */
    public boolean hasField(String fldname) {
        return this.schema.hasField(getName(fldname)) ;
    }

    /**
     * Returns the type of the specified field, using the
     * constants in {@link java.sql.Types}.
     * @param fldname the name of the field
     * @return the integer type of the field
     */
    public int type(String fldname) {
        return this.schema.type(getName(fldname)) ;
    }

    /**
     * Returns the conceptual length of the specified field.
     * If the field is not a string field, then
     * the return value is undefined.
     * @param fldname the name of the field
     * @return the conceptual length of the field
     */
    public int length(String fldname) {
        return this.schema.length(getName(fldname));
    }

    private String getName(String fldName){
        return this.aliasCollection.getOriginal(fldName);
    }

}
