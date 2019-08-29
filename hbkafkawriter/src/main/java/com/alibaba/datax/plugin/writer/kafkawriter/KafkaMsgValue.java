/**
 * KafkaMsgValue.java
 * Copyright 2019 HelloBike , all rights reserved.
 * HelloBike PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.fastjson.JSON;

import java.util.Map;

/**
 * @author lee
 * @date 2019/8/29
 */
public class KafkaMsgValue {
    private String table;
    private String primaryKey;
    private Map<String, Object> data;

    public KafkaMsgValue(String table, String primaryKey, Map<String, Object> data) {
        this.table = table;
        this.primaryKey = primaryKey;
        this.data = data;
    }

    public String toJsonString(){
        return JSON.toJSONString(this);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}
