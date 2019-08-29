package com.alibaba.datax.plugin.writer.kafkawriter.constant;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaPluginErrCode implements ErrorCode {

    REQUIRED_VALUE("KafkaWriter-00", "您缺失了必须填写的参数值."),

    COLUMN_LENGTH("KafkaWriter-01", "record的列数和配置中的table列数不匹配，请检查配置"),

    NO_PRIMARY_KEY("KafkaWriter-02", "该表未找到对应的主键");

    private final String code;

    private final String description;

    private KafkaPluginErrCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

}
