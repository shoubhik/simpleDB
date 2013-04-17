package simpledb.planner;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.parse.*;
import simpledb.server.SimpleDB;
import java.util.*;

/**
 * The simplest, most naive query planner possible.
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlanner {
   
   /**
    * Creates a query plan as follows.  It first takes
    * the product of all tables and views; it then selects on the predicate;
    * and finally it projects on the field list. 
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      //Step 1: Create a plan for each mentioned table or view
      List<Plan> plans = new ArrayList<Plan>();
      for (String tblname : data.tables()) {
         String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
         if (viewdef != null)
            plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
         else
            plans.add(new AliasTablePlanDecorator(tblname, tx,
                                                  data.getFieldAliasCollection()));
      }
      
      //Step 2: Create the product of all table plans
      Plan p = plans.remove(0);
      for (Plan nextplan : plans)
         p = new ProductPlan(p, nextplan);
      
      //Step 3: Add a selection plan for the predicate
      p = new SelectPlan(p, data.pred());
      
      //Step 4: Project on the field names
      p = new AliasProjectPlan(p, data.getFieldAliasCollection());
      return p;
   }

    @Override
    public Plan createPlan(MultiQueryData multiQueryData, Transaction tx) {
        UnionPlan unionPlan = new UnionPlan();
        QueryData createFrom = multiQueryData.getFirstQueryData();
        for(Iterator<QueryData> i = multiQueryData.iterator(); i.hasNext();){
            List<Plan> plans = new ArrayList<Plan>();
            QueryData data = i.next();
            FieldAliasCollection fieldAliasCollection = getAliases(createFrom , data);
            for (String tblname : data.tables()) {
                String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
                if (viewdef != null)
                    plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
                else
                    plans.add(new AliasTablePlanDecorator(tblname, tx,
                                                          fieldAliasCollection));
            }

            //Step 2: Create the product of all table plans
            Plan p = plans.remove(0);
            for (Plan nextplan : plans)
                p = new ProductPlan(p, nextplan);

            //Step 3: Add a selection plan for the predicate
            unionPlan.addPlan(new SelectPlan(p, data.pred()));
        }

        return new AliasProjectPlan(unionPlan, createFrom.getFieldAliasCollection());
    }

    private FieldAliasCollection getAliases(QueryData createFrom, QueryData convert){
        FieldAliasCollection fieldAliasCollection = new FieldAliasCollection();
        Collection<String> source = createFrom.getAliasFields();
        Collection<String> target = convert.fields();
        Iterator<String> i = source.iterator();
        Iterator<String> j = target.iterator();
        while(i.hasNext() && j.hasNext()) {
            String original = j.next();
            String alias = i.next();
            fieldAliasCollection.addAlias(new FieldAlias(original, alias));
        }
        return fieldAliasCollection;
    }
}
