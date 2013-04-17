package simpledb.query;

import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UnionPlan implements Plan{
    List<SelectPlan> selectPlanList;

    public UnionPlan(){
        this.selectPlanList = new ArrayList<SelectPlan>();
    }
    @Override
    public Scan open() {
        List<SelectScan> selectScanList = new ArrayList<SelectScan>();
        for(Iterator<SelectPlan> i = this.selectPlanList.iterator(); i.hasNext();){
            selectScanList.add((SelectScan)i.next().open());
        }
        return new UnionScan(selectScanList);
    }

    @Override
    public int blocksAccessed() {
        int numblks = 0;
        for(Iterator<SelectPlan> i = this.selectPlanList.iterator(); i.hasNext();)
            numblks += i.next().blocksAccessed();
        return numblks;
    }

    @Override
    public int recordsOutput() {
        int numrecs = 0;
        for(Iterator<SelectPlan> i = this.selectPlanList.iterator(); i.hasNext();)
            numrecs += i.next().recordsOutput();
        return numrecs;
    }

    @Override
    public int distinctValues(String fldname) {
        int distinct = 0;
        for(Iterator<SelectPlan> i = this.selectPlanList.iterator(); i.hasNext();)
            distinct += i.next().distinctValues(fldname);
        return distinct;
    }

    @Override
    public Schema schema() {
        // according to w3c specs
        return this.selectPlanList.get(0).schema();
    }

    public void addPlan(SelectPlan selectPlan){
        this.selectPlanList.add(selectPlan);
    }
}
