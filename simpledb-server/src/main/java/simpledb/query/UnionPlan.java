package simpledb.query;


import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class UnionPlan implements Plan{


    List<Plan> plans;
    private AliasCollection aliasCollection;
    public UnionPlan(Plan p1, Plan p2){
        plans = new ArrayList<Plan>();
        aliasCollection = new AliasCollection();
        plans.add(p1);
        plans.add(p2);
        Plan p0 = plans.get(0);

        for(int i = 1; i < plans.size(); i++){
            Plan p = plans.get(i);
            Iterator<String> original= p.schema().fields().iterator();
            Iterator<String>  alias = p0.schema().fields().iterator();
            while(original.hasNext() && alias.hasNext())
                aliasCollection.addAlias(original.next(), alias.next());
        }
    }
    @Override
    public Scan open() {
        Collection<Scan> scans = new ArrayList<Scan>();
        for(Iterator<Plan> i = plans.iterator(); i.hasNext();)
            scans.add(i.next().open());
        return new UnionScan(scans, aliasCollection);
    }

    @Override
    public int blocksAccessed() {
        int blk = 0;
        for(Iterator<Plan> i = plans.iterator(); i.hasNext();)
            blk += i.next().blocksAccessed();
        return blk;
    }

    @Override
    public int recordsOutput() {
        int out  = 0;
        for(Iterator<Plan> i = plans.iterator(); i.hasNext();)
            out += i.next().recordsOutput();
        return out;
    }

    @Override
    public int distinctValues(String fldname) {
        int distinct = 0;
        for(Iterator<Plan> i = plans.iterator(); i.hasNext();)
            distinct += i.next().distinctValues(aliasCollection.getOriginal(fldname));
        return distinct;
    }

    @Override
    public Schema schema() {
        return plans.get(0).schema();
    }

    public String toString(){
        StringBuilder sb = new StringBuilder("union ( ");
        for(Iterator<Plan> i = this.plans.iterator(); i.hasNext();){
            sb.append(i.next().toString());
            if(i.hasNext())
                sb.append(", ");
        }
        sb.append(" ) ");
        return sb.toString();
    }
}
