package simpledb.query;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AliasCollection {

    private Map<String, String> aliasMap;
    private Map<String, String> invertMap;

    public AliasCollection(){
        aliasMap = new HashMap<String, String>();
        invertMap = new HashMap<String, String>();
    }

    public void addAlias(String original, String alias){
        this.aliasMap.put(alias, original);
        this.invertMap.put(original, alias);
    }

    public String getOriginal(String alias){
        String original = this.aliasMap.get(alias);
        return original == null ? alias  : original;
    }

    public String getAlias(String original){
        String alias = this.invertMap.get(original);
        return alias ==  null ? original : alias;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<String, String>> entries = invertMap.entrySet();
        for(Map.Entry<String, String> entry : entries ){
            sb.append(entry.getKey() + " -> " +  entry.getValue() + ", ");
        }
        return sb.toString();
    }
}
