package simpledb.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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

    public String getOriginalName(String fldName){
        FieldAlias fieldAlias = this.aliasMap.get(fldName);
        return fieldAlias ==  null ? fldName : fieldAlias.getOriginal();
    }

    public Collection<String> getOriginalFields(){
        return this.reverseAlias.keySet();
    }

    public Collection<String> getAliasFields(){
        return this.aliasMap.keySet();
    }

    public String toString(){
        Iterator<String> i = this.aliasMap.keySet().iterator();
        StringBuilder sb = new StringBuilder();
        while(i.hasNext()){
            sb.append(i.next());
            if(i.hasNext()) sb.append(", ");
        }
        return sb.toString();

    }


}
