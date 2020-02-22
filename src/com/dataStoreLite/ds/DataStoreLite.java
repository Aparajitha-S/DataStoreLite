package com.dataStoreLite.ds;


import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class DataStoreLite implements IDataStoreLite {
    static final String DEFAULT_DATA_STORE_FILE = System.getProperty("user.home") + "\\DataStoreLite.txt";
    static final int MAX_KEY_LENGTH = 32;
    static final long MAX_FILE_SIZE = 1024 * 1024 * 1024;
    static final long MAX_JSON_OBJECT_SIZE = 16000;
    String filePath;
    FileLock LOCK;
    static final String DEFAULT_DELIMITER = "#@@@#";
    Logger dataStoreLog = (Logger) LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    DataStoreLite() {
        File dsFile = new File(DEFAULT_DATA_STORE_FILE);
        if (!dsFile.exists()) {
            try {
                dsFile.createNewFile();
                dataStoreLog.info("DataStore file created : " + dsFile.getAbsolutePath());
            } catch (IOException e) {
                System.out.println("Unable to create a default file ");
            }

        }
        filePath = DEFAULT_DATA_STORE_FILE;
        getFileAccess(dsFile);
    }

    private void getFileAccess(File  dsFile)  {
        FileChannel fileChannel = null;
        try {
            fileChannel = new RandomAccessFile(dsFile,"rw").getChannel();
             LOCK = fileChannel.tryLock();
        } catch (FileNotFoundException e) {
            dataStoreLog.error("Could not find the file :"+filePath);
        } catch (OverlappingFileLockException | IOException e) {
            dataStoreLog.fatal("The DataStore is already in use by another process");
        }

    }



    DataStoreLite(String customFilePath) throws IOException {
        File customFile = new File(customFilePath);
        if (!(customFile.isFile())) {
            try {
                throw new FileNotFoundException();
            } catch (FileNotFoundException e) {
                dataStoreLog.error("Could not find the file :" + customFile.getAbsolutePath());

            }

        } else {
            filePath = customFilePath;
        }
        getFileAccess(customFile);


    }


    //@Override
    private boolean isFileSizeValid(String fullFilePath) {
        File dsFile = new File(fullFilePath);
        if (dsFile.length() > (MAX_FILE_SIZE)) {
            dataStoreLog.error("The file size exceeded the maximum allowable size :"+ MAX_FILE_SIZE+" bytes");
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

    private boolean validateJSOnObject(JSONObject object) {
        String jsonString = object.toJSONString();
        boolean result=true;
        if (jsonString.getBytes().length > MAX_JSON_OBJECT_SIZE) {
            result = false;
        }
        return result;
    }

    @Override
    public HashMap<String, String> openConnection() {
        BufferedReader fileReader = null;
        HashMap<String, String> fileMap = new HashMap<>();
        try {
            fileReader = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = fileReader.readLine()) != null) {
                String[] parts = line.split(DEFAULT_DELIMITER);
                fileMap.put(parts[0], parts[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileMap;
    }

    public void printMap(HashMap<String, String> fileMap) {
        for (Map.Entry<String, String> myEntry : fileMap.entrySet()) {
            System.out.println(myEntry.getKey());
            System.out.println(myEntry.getValue());
        }

    }
    public void create(String key,JSONObject value){
        create(key,value,0);
    }

    //@Override
    public void create(String key, JSONObject value, int timeToLive) {
        if (!validateKey(key)) {
            dataStoreLog.error("The key is not valid : "+key);

        }
        if(!validateJSOnObject(value)){
            dataStoreLog.error("The JSON Object is not valid: "+value.toJSONString());
        }
        HashMap<String, String> myMap = openConnection();
        Set keys = myMap.keySet();
        for (Object myKey : keys) {
            if (key.equals(myKey)) {
                dataStoreLog.error("The key provided is already available: "+key);
            }
        }

        try {
            BufferedWriter bWriter = new BufferedWriter(new FileWriter(filePath, true));
            synchronized (this) {
                bWriter.write(key);
                bWriter.write(DEFAULT_DELIMITER);
                bWriter.write(value.toJSONString());
                bWriter.write(DEFAULT_DELIMITER);
                bWriter.write(String.valueOf(System.currentTimeMillis()));
                bWriter.write(DEFAULT_DELIMITER);
                bWriter.write(timeToLive);
                bWriter.newLine();
            }
            bWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public JSONObject read(String key) {

        HashMap<String, String> myMap = new HashMap<String, String>();
        myMap = openConnection();
        String returnString = myMap.get(key);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject = (JSONObject) jsonParser.parse(returnString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return jsonObject;
    }

    @Override
    public void delete(String key) {
        HashMap<String, String> myMap = openConnection();
        myMap.remove(key);
        try {
            BufferedWriter bWriter = new BufferedWriter(new FileWriter(filePath));


            for (Map.Entry<String, String> mapElement : myMap.entrySet()) {
                String MYkey = (String) mapElement.getKey();
                String value = (String) mapElement.getValue();
                synchronized (this) {
                    bWriter.write(MYkey);
                    bWriter.write(DEFAULT_DELIMITER);
                    bWriter.write(value);
                    bWriter.newLine();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }


    }


}
