package com.dataStoreLite.ds;

import org.json.simple.JSONObject;

public interface IDataStoreLite {
    void create(String key, JSONObject value);

    JSONObject read(String key);

    void delete(String key);
}
