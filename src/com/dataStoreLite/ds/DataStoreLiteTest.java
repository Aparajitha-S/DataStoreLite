package com.dataStoreLite.ds;


import org.apache.logging.log4j.LogManager;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;


public class DataStoreLiteTest {
  //  private static final Logger DSTLOG = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);
    private static final Logger DSTLOG = LogManager.getLogger(DataStoreLiteTest.class);

    public static void main(String args[]){


        DSTLOG.trace("DataStoreLite Test Framework Started: ");
        DataStoreLiteTest DSTest = new DataStoreLiteTest();
       // DSTest.createDataTest();
       // DSTest.readDataTest();
        DSTest.deleteDataTest();
    }

    private void createDataTest() {
        DataStoreLite dsLite = null;
        // Test with custom file path - Invalid file
        dsLite = new DataStoreLite("C:\\DataStoreLite\\DSLite.txt");

        // Test with default file path
        //dsLite = new DataStoreLite();

        JSONObject myJsonObject = new JSONObject();
        // JSON object with 2 key-value pairs.
        myJsonObject.put("name01","Name001");
        myJsonObject.put("name02","Name002");

        //Large JSON Object
        JSONObject myJsonObjectLarge = new JSONObject();
        for(int i=0;i<10000;i++){
            myJsonObjectLarge.put("Name000_"+String.valueOf(i),"ValueName001_"+String.valueOf(i));
        }

        dsLite.create("DSL001",myJsonObject);
        dsLite.create("DSL_1",myJsonObjectLarge,20000);
        dsLite.create("DSL_2",myJsonObject,30);
        dsLite.create("DSL_3",myJsonObject,5);



    }

    private void readDataTest(){
        DataStoreLite dsLite = new DataStoreLite();
        DSTLOG.info(dsLite.read("DSL0005").toJSONString());
    }

    private void deleteDataTest(){
        DataStoreLite dsLite = new DataStoreLite();
        dsLite.delete("DSL0005");
        dsLite.delete("DSL_1");
        dsLite.delete("This key is Invalid");

    }
}
