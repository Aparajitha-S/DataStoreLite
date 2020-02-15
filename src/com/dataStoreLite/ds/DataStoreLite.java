package com.dataStoreLite.ds;


import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.github.cliftonlabs.json_simple.JsonObject;

public class DataStoreLite implements IDataStoreLite {
    static final String DEFAULT_DATA_STORE_FILE = System.getProperty("user.home") + "\\DataStoreLite.txt";
    static final int MAX_KEY_LENGTH = 32;
    static final long MAX_FILE_SIZE = 1024 * 1024 * 1024;
    String filePath;
    static final String DEFAULT_DELIMITER = "#@@@#";

    DataStoreLite() {
        File dsFile = new File(DEFAULT_DATA_STORE_FILE);
        if (!dsFile.exists()) {
            try {
                dsFile.createNewFile();
                System.out.println("File created  :" + dsFile.getAbsolutePath());
            } catch (IOException e) {
                System.out.println("Unable to create a default file ");
            }

        }
        filePath = DEFAULT_DATA_STORE_FILE;

    }

    DataStoreLite(String customFilePath) {
        File customFile = new File(customFilePath);
        if (!(customFile.isFile())) {
            try {
                throw new FileNotFoundException();
            } catch (FileNotFoundException e) {
                System.out.println("Error : Not a valid File ");
                // e.printStackTrace();
            }

        }
        else{
            filePath = customFilePath;
        }



    }




    //@Override
    private boolean isFileValid(String fullFilePath) {
        File dsFile = new File(fullFilePath);
        if (dsFile.length() > (MAX_FILE_SIZE)) {
            try {
                throw new DataStoreFileExceedException("File Size exceeded " + MAX_FILE_SIZE);
            } catch (DataStoreFileExceedException e) {
                e.printStackTrace();
            }

        }


        return true;
    }



    private boolean validateKey(String key) {
        boolean isValidKey = true;
        if (key.length() > MAX_KEY_LENGTH) {
            isValidKey = false;
        }

        return isValidKey;
    }


    @Override
    public HashMap<String,String> openConnection() {
        BufferedReader fileReader = null;
        HashMap<String,String> fileMap = new HashMap<String,String>();
        try {
            fileReader = new BufferedReader(new FileReader(filePath));
            String line;
            while((line = fileReader.readLine())!= null){
                String[] parts = line.split(DEFAULT_DELIMITER);
                fileMap.put(parts[0],parts[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileMap;
    }

    public void printMap(HashMap<String,String> fileMap){
        for(Map.Entry<String,String> myEntry : fileMap.entrySet()){
            System.out.println(myEntry.getKey());
            System.out.println(myEntry.getValue());
        }

    }

 //@Override
    public void create(String key, JsonObject value) {

        try {
            BufferedWriter bWriter = new BufferedWriter(new FileWriter(filePath,true));
            synchronized (this) {

                bWriter.write(key);
                bWriter.write(DEFAULT_DELIMITER);
                bWriter.write(value.toJson());
                bWriter.newLine();
            }
            bWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public JsonObject read(String key) {
        HashMap<String, String> myMap = new HashMap<String,String>();
        myMap = openConnection();

      String returnString = myMap.get(key);
      
      JsonObject myObject = new JsonObject(returnString);








    }

    @Override
    public void delete(String key) {

    }


}
