package simpledb.query;

import java.util.Iterator;
import java.util.List;

/**
 * User: shoubhik Date: 16/4/13 Time: 5:03 PM
 */
public class UnionScan implements Scan{

    private List<SelectScan> selectScans;
    private int idx;
    public UnionScan(List<SelectScan> selectScans){
        if(selectScans == null || selectScans.size() == 0)
            throw new IllegalArgumentException();
        this.selectScans = selectScans;
        this.idx = 0;
    }



    @Override
    public void beforeFirst() {
        for(Iterator<SelectScan> i = this.selectScans.iterator(); i.hasNext();){
            i.next().beforeFirst();
        }
    }

    @Override
    public boolean next() {
        if(this.selectScans.get(idx).next())
            return true;
        if(++idx == this.selectScans.size())
            return false;
        return this.selectScans.get(idx).next();
    }

    @Override
    public void close() {
        for(Iterator<SelectScan> i = this.selectScans.iterator(); i.hasNext();){
            i.next().close();
        }
    }

    @Override
    public Constant getVal(String fldname) {
        return this.selectScans.get(idx).getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return this.selectScans.get(idx).getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return this.selectScans.get(idx).getString(fldname);
    }

    /**
     * according to specs the fieldname will always correspond to the first select
     * in our case it is inverted so it will be the last select.
     * @param fldname the name of the field
     * @return
     */
    @Override
    public boolean hasField(String fldname) {
        return this.selectScans.get(this.selectScans.size() - 1).hasField(fldname);
    }

    @Override
    public boolean isNull(String fldName) {
        return this.selectScans.get(idx).isNull(fldName);
    }
}
