package test;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import static junit.framework.Assert.assertTrue;

/**
 * User: shoubhik Date: 19/4/13 Time: 4:28 PM
 */
public class QueryTest {

    @Before
    public void initSimpleDb(){
        SimpleDB.init("studentdb");
    }

    @Test
    public void testTable() {
        Transaction tx = new Transaction();
        Plan p = new TablePlan("student", tx);
        Scan s = p.open();
        int id = 0;
        while (s.next()) {
            if (id != s.getInt("sid")
                    || !s.getString("sname").equals("student"+id)
                    ||  s.getInt("gradyear") != id%50+1960)
                System.out.println("*****QueryTest: bad table scan");
            id++;
        }
        s.close();
        tx.commit();
    }

    @Test
    public void testSelect() {
        Transaction tx = new Transaction();
        Plan p1 = new TablePlan("student", tx);
        Expression exp1 = new FieldNameExpression("majorid");
        Expression exp2 = new ConstantExpression(new IntConstant(30));
        Term t = new Term(exp1, exp2);
        Predicate pred = new Predicate(t);
        Plan p2 = new SelectPlan(p1, pred);
        Scan s = p2.open();
        while (s.next())
            if (s.getInt("majorid") != 30)
                System.out.println("*****QueryTest: bad select scan");
        s.close();
        tx.commit();
    }

    @Test
    public void testProduct() {
        Transaction tx = new Transaction();
        Plan p1 = new TablePlan("student", tx);
        Plan p2 = new TablePlan("dept", tx);
        Plan p3 = new ProductPlan(p1, p2);
        Expression exp1 = new FieldNameExpression("majorid");
        Expression exp2 = new FieldNameExpression("did");
        Predicate pred = new Predicate(new Term(exp1, exp2));
        Plan p4 = new SelectPlan(p3, pred);
        Scan s1 = p1.open();
        int count1 = 0;
        while (s1.next())
            count1++;
        s1.close();
        Scan s2 = p2.open();
        int count2 = 0;
        while (s2.next())
            count2++;
        s2.close();
        Scan s3 = p3.open();
        int count3 = 0;
        while (s3.next())
            count3++;
        s3.close();
        Scan s4 = p4.open();
        int count4 = 0;
        while (s4.next())
            count4++;
        s4.close();
        System.out.println("plan::" + p4.toString());
        if (count3 != count1 * count2
                || count4 != count1)
            System.out.println("*****QueryTest: bad product scan");
        tx.commit();
    }

    @Test
    public void testUnionAllRecords(){
        Transaction tx = new Transaction();
        Plan p1 = new TablePlan("test", tx);
        Plan p2 = new TablePlan("test1", tx);
        Plan p3 = new UnionPlan(p1, p2);
        Scan s1 = p1.open();
        Scan s2= p2.open();
        int count1 = 0 , count2 = 0, count3 = 0;
        while(s1.next())
            count1++;
        while(s2.next())
            count2++;
        Scan s3 = p3.open();
        while(s3.next())
            count3++;
        assertTrue(count3 == (count1 + count2));
    }

    @Test
    public void testRename(){
        Transaction tx = new Transaction();
        Plan p = new RenamePlan("test", tx, "id", "bankai");
        Scan s = p.open();
        while(s.next()) System.out.println(s.getInt("bankai"));
        System.out.println(p.schema().fields());
        assertTrue(true);
    }

    @Test
    public void writeNull(){
        Transaction tx = new Transaction();
        Plan p = new TablePlan("test", tx);
        UpdateScan s = (UpdateScan)p.open();
        s.insert();
        s.setVal("id",new NullConstant());
        s.close();
        tx.commit();
    }

    @Test
    public void readNull(){
        Transaction tx = new Transaction();
        Plan p = new TablePlan("test", tx);
        Scan s = p.open();
        while(s.next()){
            Constant c = s.getVal("id");
            System.out.println(c.asJavaVal());
        }
        s.close();
        tx.commit();

    }
}
