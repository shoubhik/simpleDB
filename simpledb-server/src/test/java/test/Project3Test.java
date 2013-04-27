package test;

import java.util.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.record.TableInfo;

public class Project3Test {
    public static void main(String[] args) {
        SimpleDB.init("Project3testdb");
        String tblname = "T";
        Transaction tx = new Transaction();
//        createTable(tblname, tx);
//        tx.commit();
        tx = new Transaction();
        Plan p = new TablePlan(tblname, tx);
        query1(p);
        query2(p);
//        addNullValues(p);
//        printAlRecords(p);
//        queryForNull(p);
//        countNulls(p);
//        query3(p);
        tx.commit();

    }

    private static void createTable(String tblname, Transaction tx) {
        MetadataMgr mdmgr = SimpleDB.mdMgr();
        TableInfo ti = mdmgr.getTableInfo(tblname, tx);
        if (ti.recordLength() > 0)
            return;  // Use existing table

        // Create a table T(A int, B varchar(15));
        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 15);
        mdmgr.createTable(tblname, sch, tx);

        // Populate the table
        Plan p = new TablePlan(tblname, tx);
        UpdateScan us = (UpdateScan) p.open();
        for (int i=0; i<300; i++) {
            us.insert();
            us.setInt("A", i);
            us.setString("B", "b"+(i%20));
        }
        us.close();
    }

    private static void query1(Plan p) {
        // Query 1: Print the A-values of records
        // having the same B-value as record 33.

        //A predicate corresponding to "A = 33".
        Expression lhs = new FieldNameExpression("A");
        Constant c = new IntConstant(33);
        Expression rhs = new ConstantExpression(c);
        Term t = new Term(lhs, rhs);
        Predicate selectpred = new Predicate(t);

        // A predicate corresponding to "B = Bof33"
        Expression lhs2 = new FieldNameExpression("B");
        Expression rhs2 = new FieldNameExpression("Bof33");
        Term t2 = new Term(lhs2, rhs2);
        Predicate joinpred = new Predicate(t2);

        Plan p2 = new SelectPlan(p, selectpred);
        Collection<String> flds = Arrays.asList("B");
        Plan p3 = new ProjectPlan(p2, flds);
        Plan p4 = new RenamePlan(p3, "B", "Bof33");
        Plan p5 = new ProductPlan(p, p4);
        Plan p6 = new SelectPlan(p5, joinpred);

        print(p6, "Here are the records that have the same B-value as record 33:");
    }

    private static void printAlRecords(Plan p){

        Collection<String> flds = Arrays.asList("A","B");
        Plan p3 = new ProjectPlan(p, flds);
        Scan s = p3.open();
        while(s.next()){
            Constant c1 = s.getVal("A");
            Constant c2 = s.getVal("B");
            System.out.println(c1.asJavaVal() + "\t\t" +c2.asJavaVal());
        }

    }

    private static void query2(Plan p) {
        // Query 2: Print the A-values of records
        // whose B-values are either "b1" or "b9".

        // A select predicate for "B = 'b1' "
        Expression lhs = new FieldNameExpression("B");
        Constant c = new StringConstant("b1");
        Expression rhs = new ConstantExpression(c);
        Term t = new Term(lhs, rhs);
        Predicate pred1 = new Predicate(t);

        // A select predicate for "B = 'b9' "
        Expression lhs2 = new FieldNameExpression("B");
        Constant c2 = new StringConstant("b9");
        Expression rhs2 = new ConstantExpression(c2);
        Term t2 = new Term(lhs2, rhs2);
        Predicate pred2 = new Predicate(t2);

        Plan p2 = new SelectPlan(p, pred1);
        Plan p3 = new SelectPlan(p, pred2);
        Plan p4 = new UnionPlan(p2, p3);

        print(p4, "Here are the records that have the B-value 'b1' or 'b9': ");
    }

    private static void query3(Plan p) {
        // Query 2: Print the A-values of records
        // whose B-values are either "b1" or "b9".

        // A select predicate for "B = 'b1' "
        Expression lhs = new FieldNameExpression("B");
        Constant c = new StringConstant("b1");
        Expression rhs = new ConstantExpression(c);
        Term t = new Term(lhs, rhs);
        Predicate pred1 = new Predicate(t);

        // A select predicate for "B = 'b9' "
        Expression lhs2 = new FieldNameExpression("B");
        Constant c2 = new StringConstant("b9");
        Expression rhs2 = new ConstantExpression(new NullConstant());
        Term t2 = new Term(lhs2, rhs2);
        Predicate pred2 = new Predicate(t2);

        Plan p2 = new SelectPlan(p, pred1);
        Plan p3 = new SelectPlan(p, pred2);
        Plan p4 = new UnionPlan(p2, p3);

        print(p4, "Here are the records that have the B-value 'b1' or B is null: ");
        System.out.println("str = "  + p4.toString().replace(" ", ""));
    }

    private static void print(Plan p, String msg) {
        System.out.println(msg);
        Scan s = p.open();
        while (s.next()) {
            Constant a = s.getVal("A");
            System.out.print(a.asJavaVal() + " ");
        }
        s.close();
        System.out.println("\n");

    }

    public static void addNullValues(Plan p){
        UpdateScan us = (UpdateScan) p.open();
        us.insert();
        us.setVal("A", new NullConstant());
        us.setVal("B", new StringConstant("bankai"));

        us.insert();
        us.setVal("A", new IntConstant(400));
        us.setVal("B", new NullConstant());


        us.insert();
        us.setVal("A", new NullConstant());
        us.setVal("B", new NullConstant());
    }

    public static void queryForNull(Plan p){
        Expression lhs = new FieldNameExpression("A");
        Constant c = new NullConstant();
        Expression rhs = new ConstantExpression(c);
        Term t = new Term(lhs, rhs);
        Predicate pred1 = new Predicate(t);
        Plan p2 = new SelectPlan(p, pred1);

        print(p2, "Null values where B is null are: ");

    }

    public static void countNulls(Plan p){
        Expression lhs = new FieldNameExpression("A");
        Constant c = new NullConstant();
        Expression rhs = new ConstantExpression(c);
        Term t = new Term(lhs, rhs);
        Predicate pred1 = new Predicate(t);
        Plan p2 = new SelectPlan(p, pred1);
        int count = 0;
        Scan s = p2.open();
        while(s.next()) count++;
        System.out.println("num records where A is null is :"  + count);
        count = 0;

        Expression lhs1 = new FieldNameExpression("B");
        Constant c1 = new NullConstant();
        Expression rhs1 = new ConstantExpression(c1);
        Term t1 = new Term(lhs1, rhs1);
        Predicate pred2 = new Predicate(t1);
        pred1.conjoinWith(pred2);
        Plan p3 = new SelectPlan(p, pred1);
        Scan s1 = p3.open();
        while(s1.next()) count++;
        System.out.println("num records where A and B are null is :"  + count);
    }
}