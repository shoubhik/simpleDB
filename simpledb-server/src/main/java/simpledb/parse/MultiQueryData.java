package simpledb.parse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * User: shoubhik Date: 16/4/13 Time: 10:58 PM
 */
public class MultiQueryData {
    List<QueryData> queryDataList;

    public MultiQueryData(){
        this.queryDataList = new ArrayList<QueryData>();
    }

    public void addQueryData(QueryData data){
        this.queryDataList.add(data);
    }

    public Iterator<QueryData> iterator(){
        return this.queryDataList.iterator();
    }

    public QueryData getFirstQueryData(){
        return this.queryDataList.get(0);
    }
}

