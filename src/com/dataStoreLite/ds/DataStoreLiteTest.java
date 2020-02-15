package com.dataStoreLite.ds;
import com.dataStoreLite.ds.DataStoreLite;
import com.github.cliftonlabs.json_simple.JsonObject;

public class DataStoreLiteTest {
    public static void main(String args[]){
        DataStoreLite ds = new DataStoreLite();
      //  ds.printMap(ds.openConnection());
        JsonObject jsonObject = new JsonObject();
        for(int i=0;i<10;i++) {
            synchronized (jsonObject) {
                String key1 = String.valueOf(i);
                String key2 = String.valueOf(i +200);
                jsonObject.put(key1, "Aparajitha");
                jsonObject.put(key2, "32");
            }
        }
        ds.create("001",jsonObject);
        ds.printMap(ds.openConnection());
        ds.create("002",jsonObject);
        ds.printMap(ds.openConnection());



    }
}
