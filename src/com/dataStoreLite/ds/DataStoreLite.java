package com.dataStoreLite.ds;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static com.dataStoreLite.ds.DataStoreLiteTimer.scheduleTask;


public class DataStoreLite implements IDataStoreLite {
    static final String DEFAULT_DATA_STORE_FILE = System.getProperty("user.home") + "\\DataStoreLite.txt"; // Default DataStore file location
    static final int MAX_KEY_LENGTH = 32; // Maximum allowable key length (32 chars)
    static final long MAX_FILE_SIZE = 1024 * 1024 * 1024; // Maximum file size (1GB)
    static final long MAX_JSON_OBJECT_SIZE = 16000; // Maximum JSON data size 16KB
    /* The Data will be stored in file in the following format
    Key[DEFAULT_DELIMITER]Value[DATA_DELIMITER]TimeToLive[DATA_DELIMITER]CreatedTimeInMilliSeconds
     */
    static final String DEFAULT_DELIMITER = "#@@@#";
    static final String DATA_DELIMITER = "###";
    static ConcurrentHashMap<String, String> keyMap; // Map that keeps track of key with Expired Time
    static ConcurrentHashMap<String, String> DSCache; // Local Cache that maintains data for which time to live property is valid

    static {
        keyMap = new ConcurrentHashMap<>();
        DSCache = new ConcurrentHashMap<>();
        scheduleTask(); // Run a task to remove the expired keys from the DSCache regularly.
    }

    String filePath; // File location for DataStore
    FileLock LOCK; // Lock for maintaining single process multi thread mechanism
    static final Logger DSLog = (Logger) LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    /**
     * Initialize DataStore
     * If no specific file path is provided, create a file with default path
     */
    DataStoreLite() {
        File dsFile = new File(DEFAULT_DATA_STORE_FILE);
        if (!dsFile.exists()) {
            try {
                dsFile.createNewFile();
                DSLog.info("DataStore file created : " + dsFile.getAbsolutePath());
            } catch (IOException e) {
                DSLog.error("Unable to create a default file ");
                System.exit(1);
            }
        }
        filePath = DEFAULT_DATA_STORE_FILE;

    }

    /**
     * Initialize DataStore with custom file path.
     *
     * @param customFilePath File path for DataStore
     *                       Throws Error when the file is not found at the given location.
     */

    DataStoreLite(String customFilePath) {
        File customFile = new File(customFilePath);
        if (!(customFile.isFile() && customFile.exists())) {
            try {
                throw new FileNotFoundException();
            } catch (FileNotFoundException e) {
                DSLog.error("Could not find the file :" + customFile.getAbsolutePath());
                System.exit(1);
            }
        } else {
            filePath = customFilePath;
        }
    }

    /*
    Method to remove the expired keys from KeyMap and DSCache
    KeyMap - Stores the keys which have valid timeToLive property with their expiry time in milliseconds
    KeyMap - < Key, [Expiry time in milliseconds]>
    DSCache - Stores the key value pairs which have valid timeToLive Property specified
     */

    protected static void removeExpiredKeys() {
        DSLog.info("Removing Expired Keys .....");
        if(!keyMap.isEmpty()) {
            for (Map.Entry<String, String> expKey : keyMap.entrySet()) {
                DSLog.trace("key :" + expKey.getKey());
                if (Long.parseLong(expKey.getValue()) < System.currentTimeMillis()) {
                    keyMap.remove(expKey.getKey());
                    DSCache.remove(expKey.getKey());
                }
            }
        }

    }

    /*
    Method to get exclusive access for File to prevent another process from accessing the file.
     */

    private void getFileAccess(FileReader dsFile) {
        FileChannel fileChannel = null;
        try {
            fileChannel = new RandomAccessFile(String.valueOf(dsFile), "rw").getChannel();
            LOCK = fileChannel.tryLock();
            DSLog.trace(LOCK.isValid());
        } catch (FileNotFoundException e) {
            DSLog.error("Could not find the file :" + filePath);
        } catch (OverlappingFileLockException | IOException e) {
            DSLog.fatal("File Locked !!!");
        }

    }

    /*
    Method to release File Lock after completing read/write operations.
     */

    private void releaseFileAccess() {
        if (LOCK.isValid()) {
            try {
                LOCK.close();
            } catch (IOException e) {
                DSLog.error("Could'nt release the lock!!");
            }
        }
    }

    /*
    Method to  verify if the File size exceeded Maximum allowable file size.
    Return true if the File size is valid
    If the file size is invalid, return false and throw error message.

     */
    private boolean isFileSizeValid(String fullFilePath) {
        File dsFile = new File(fullFilePath);
        if (dsFile.length() > (MAX_FILE_SIZE)) {
            try {
                throw new DataStoreLiteException("The file size exceeded the maximum allowable size " +
                        ":" + MAX_FILE_SIZE + " bytes");
            } catch (DataStoreLiteException e) {
                e.printStackTrace();
            }
            DSLog.error("The file size exceeded the maximum allowable size :"
                    + MAX_FILE_SIZE + " bytes");
            return false;
        }
        return true;
    }

    /*
     Validate if the key length is within limit MAX_KEY_LENGTH
     Return false if key length exceeded MAX_KEY_LENGTH.
     */

    private boolean validateKey(String key) {
        boolean isValidKey = true;
        if (key.length() > MAX_KEY_LENGTH) {
            isValidKey = false;
        }
        return isValidKey;
    }

    /*
     Validate if the JSONObject length is within limit MAX_JSON_OBJECT_SIZE
     Return false if JSONObject length exceeded MAX_JSON_OBJECT_SIZE.
     */

    private boolean validateJSOnObject(JSONObject object) {
        String jsonString = object.toJSONString();
        boolean result = true;
        if (jsonString.getBytes().length > MAX_JSON_OBJECT_SIZE) {
            result = false;
        }
        return result;
    }

    /*
    openConnection()
    Read Data from File and added to a HashMap
    Data is read line by line
    Each line is split into key value pairs and added to HashMap.
     */


    private ConcurrentHashMap<String, String> openConnection() {
        BufferedReader fileReader = null;
        ConcurrentHashMap<String, String> fileMap = new ConcurrentHashMap<>();
        if (nonEmptyFile() && isFileSizeValid(filePath)) {
            try {

                FileReader myFile = new FileReader(filePath);
                getFileAccess(myFile);
                fileReader = new BufferedReader(myFile);
                String line;
                while ((line = fileReader.readLine()) != null) {
                    String[] parts = line.split(DEFAULT_DELIMITER);
                    fileMap.put(parts[0], parts[1]);
                }
                releaseFileAccess();
                fileReader.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        return fileMap;
    }

    /*
    Method to check if the key has a valid timeToLive Property
     */

    private boolean isTimeToLiveValid(String key) {
        boolean isValid = false;
        long timeToLive;
        timeToLive = Long.parseLong(getDataFromString(key, 1));
        DSLog.trace("key:" + key + "timetToLive: " + timeToLive);
        long timeElapsed = System.currentTimeMillis() - Long.parseLong(getDataFromString(key, 2));
        if (timeElapsed < timeToLive) {
            isValid = true;
        }
        return isValid;
    }
    /*
    Method to verify if the File is empty or not.

     */

    private boolean nonEmptyFile() {
        boolean result = true;

        File myFile = new File(filePath);
        DSLog.trace("File Length : " + myFile.length());
        if (myFile.length() == 0) {
            result = false;
        }
        return result;
    }

    /*
    Print HashMap
     */

    protected void printMap(ConcurrentHashMap<String, String> fileMap) {
        for (Map.Entry<String, String> myEntry : fileMap.entrySet()) {
            DSLog.trace(myEntry.getKey());
            DSLog.trace(myEntry.getValue());
        }

    }

    /**
     * Create Data in the Key Value Data Store
     * The Data Store accepts key which is unique.
     *
     * @param key   String with maximum length of 32 Characters
     * @param value JSON Object with Maximum size of 16KB
     */

    public void create(String key, JSONObject value) {
        create(key, value, 0);
    }

    /**
     * @param key        String with maximum length of 32 Characters
     * @param value      JSON Object with Maximum size of 16KB
     * @param timeToLive Integer representing the number of seconds
     *                   the key should be retained in the data store
     *                   for read and delete operations after which the key
     *                   gets expired.
     */

    //@Override
    public void create(String key, JSONObject value, int timeToLive) {
        if (!validateKey(key)) {
            DSLog.error("The key is not valid : " + key);
            try {
                throw new DataStoreLiteException("The key is not valid : " + key);
            } catch (DataStoreLiteException e) {
                DSLog.error("Provide a valid key");
            }
            return;

        }
        if (!validateJSOnObject(value)) {
            try {
                throw new DataStoreLiteException("The JSON Object is not valid: " + value.toJSONString());
            } catch (DataStoreLiteException e) {
               DSLog.error("Provide a valid JSON Object");
            }
            DSLog.error("The JSON Object is not valid: " + value.toJSONString());
            return;
        }
        ConcurrentHashMap<String, String> myMap = openConnection();
        Set keys = myMap.keySet();
        for (Object myKey : keys) {
            if (key.equals(myKey)) {
                try {
                    throw new DataStoreLiteException("The key provided is already available: " + key);
                } catch (DataStoreLiteException e) {
                    DSLog.error("Key should be unique");
                }
                DSLog.error("The key provided is already available: " + key);
                return;
            }
        }
        /*
        If time to live property is a positive value, add the key value pairs to Data Store Chache
        Add key and time of Expiry (Created time + time to Live ) in milliseconds to keyMap.
        This will be further utilised by dataStore to keep tack of keys in DSCache and
        remove them after expiry.
         */

        try {
            if (timeToLive > 0) {
                DSCache.put(key, value.toJSONString());
                keyMap.put(key, String.valueOf(System.currentTimeMillis() + timeToLive * 1000));
            }
            /*
            Write Data to file in the format
            <key><DEFAULT_DELIMITER><Value><DATA_DELIMITER><timeToLive><DATA_DELIMITER><Created time in milliseconds>
             */

            BufferedWriter bWriter = new BufferedWriter(new FileWriter(filePath, true));
            synchronized (this) {
                bWriter.write(key);
                bWriter.write(DEFAULT_DELIMITER);
                bWriter.write(value.toJSONString());
                bWriter.write(DATA_DELIMITER);
                bWriter.write(String.valueOf(timeToLive));
                bWriter.write(DATA_DELIMITER);
                bWriter.write(String.valueOf(System.currentTimeMillis()));
                bWriter.newLine();
            }
            bWriter.flush();
            bWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        closeConnection();


    }

    /**
     * Read the value from dataStore and return a JSONObject.
     *
     * @param key String for the required JSON Object
     * @return JSON Object from dataStore
     * Error message is displayed if the key is not found in the dataStore.
     */

    @Override
    public JSONObject read(String key) {
        if(!validateKey(key)){
            DSLog.error("Key is Invalid :");
            try {
                throw new DataStoreLiteException(key);
            } catch (DataStoreLiteException e) {
                e.printStackTrace();
            }
        }
        String returnString = "";
        JSONObject jsonObject = new JSONObject();
        JSONParser jsonParser = new JSONParser();
        if(!DSCache.isEmpty()) {
            returnString = DSCache.get(key); // Check for key in DSCache
        }
        if (returnString == null) {
            // Read data from Datastore for the give key
            ConcurrentHashMap<String, String> myMap = openConnection();
            if(myMap.isEmpty()){
                try {
                    throw new DataStoreLiteException("Could not read data from dataStore");
                } catch (DataStoreLiteException e) {
                    e.printStackTrace();
                }
            }else {
                returnString = myMap.get(key);
            }
        }
        DSLog.info(returnString);

        if (returnString == null) {
            // Error message if key is not found.
            DSLog.error("Could'nt find the key in Data Store");
        } else {
            try {
                // Parse JSON String to JSONObject and return.
                String dataValue = getDataFromString(returnString, 0);
                jsonObject = (JSONObject) jsonParser.parse(dataValue);
            } catch (ParseException e) {
                DSLog.error("Exception occurred while parsing JSON String");
            }
        }
        closeConnection();


        return jsonObject;

    }

    /*
    Method to split the sting in format
    <Value><DATA_DELIMITER><timeToLive><DATA_DELIMITER><Created time in milliseconds>
    and retrieve individual values.
     */

    private String getDataFromString(String key, int position) {
        String result;
        String[] values = key.split(DATA_DELIMITER);
        if (position < 3 && position >= 0) {
            result = values[position];
        } else {
            result = "";
        }
        return result;


    }

    /**
     * Delete Data from DataStore for given key
     *
     * @param key Parameter to remove data. (String)
     */

    @Override
    public void delete(String key) {
        if(!validateKey(key)){
            DSLog.error("Key is Invalid :");
            try {
                throw new DataStoreLiteException(key);
            } catch (DataStoreLiteException e) {
                DSLog.error("Invalid Key :"+key);
            }
        }
        ConcurrentHashMap<String, String> myMap = openConnection();
        if(myMap.isEmpty()){
            try {
                throw new DataStoreLiteException("Could not Delete data from dataStore");
            } catch (DataStoreLiteException e) {
                DSLog.error("Unable to delete :"+key);
            }
        }
        keyMap.remove(key);
        DSCache.remove(key);
        if(!myMap.isEmpty()) {
            myMap.remove(key);
            try {
                BufferedWriter bWriter = new BufferedWriter(new FileWriter(filePath));
                for (Map.Entry<String, String> mapElement : myMap.entrySet()) {
                    String MYkey = mapElement.getKey();
                    String value = mapElement.getValue();
                    synchronized (this) {
                        bWriter.write(MYkey);
                        bWriter.write(DEFAULT_DELIMITER);
                        bWriter.write(value);
                        bWriter.newLine();
                    }
                }
                bWriter.flush();
                bWriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            closeConnection();
        }


    }
    /*
    If a valid lock is present, close the connection
     */


    private void closeConnection() {
        try {
            if (LOCK != null) {
                LOCK.close();
            }
        } catch (IOException e) {
            DSLog.error("I/O Exception occurred.. ");
        }
    }


}
