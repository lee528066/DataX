package com.alibaba.datax.plugin.writer.kafkawriter.constant;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaErrorCode implements ErrorCode {

    REQUIRED_VALUE("KafkaWriter-00", "您缺失了必须填写的参数值.");

    private final String code;

    private final String description;

    private KafkaErrorCode(String code, String description) {
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
