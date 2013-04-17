package simpledb.query;

import java.util.HashMap;
import java.util.Map;

public class FieldAliasCollection {

    Map<String, FieldAlias> aliasMap;
    Map<String, String> reverseAlias;

    public FieldAliasCollection(){
        this.aliasMap = new HashMap<String, FieldAlias>();
        this.reverseAlias = new HashMap<String, String>();
    }

    public void addAlias(FieldAlias fieldAlias){
        this.aliasMap.put(fieldAlias.getAlias(), fieldAlias);
        this.reverseAlias.put(fieldAlias.getOriginal(), fieldAlias.getAlias());
    }

    public String getAliasName(String fldName){
        FieldAlias fieldAlias = this.aliasMap.get(fldName);
        return fieldAlias ==  null ? fldName : fieldAlias.getOriginal();
    }


}
