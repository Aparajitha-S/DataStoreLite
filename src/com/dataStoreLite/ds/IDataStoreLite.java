package com.dataStoreLite.ds;

import com.github.cliftonlabs.json_simple.JsonObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

public interface IDataStoreLite {

    public HashMap openConnection() throws IOException;
    //public boolean isFileValid(String completeFileLocation);
    //boolean validateKey(String key);
    public void create(String key, JsonObject value);
    public JsonObject read(String key);
    public void delete(String key);





}
