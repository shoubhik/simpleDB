package simpledb.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * User: shoubhik Date: 19/4/13 Time: 5:35 PM
 */
public class UnionScan implements Scan {

    private List<Scan> scans;
    private int idx;
    private AliasCollection aliasCollection;

    public UnionScan(Collection<Scan> scans, AliasCollection aliasCollection){
        this.scans = new ArrayList<Scan>();
        this.scans.addAll(scans);
        this.aliasCollection = aliasCollection;
        this.idx = 0;
    }
    @Override
    public void beforeFirst() {
        for(Iterator<Scan> i = scans.iterator(); i.hasNext();)
            i.next().beforeFirst();
    }

    @Override
    public boolean next() {
        if(scans.get(idx).next()) return true;
        idx++;
        if(idx == scans.size())
            return false;
        return scans.get(idx).next();
    }

    @Override
    public void close() {
        for(Iterator<Scan> i = scans.iterator(); i.hasNext();)
            i.next().close();
    }

    @Override
    public Constant getVal(String fldname) {
        return scans.get(idx).getVal(getOriginalField(fldname));
    }

    @Override
    public int getInt(String fldname) {
        return scans.get(idx).getInt(getOriginalField(fldname));
    }

    @Override
    public String getString(String fldname) {
        return scans.get(idx).getString(getOriginalField(fldname));
    }

    @Override
    public boolean hasField(String fldname) {
        return scans.get(idx).hasField(getOriginalField(fldname));
    }

    private String getOriginalField(String fldName){
        return this.aliasCollection.getOriginal(fldName);
    }
}
