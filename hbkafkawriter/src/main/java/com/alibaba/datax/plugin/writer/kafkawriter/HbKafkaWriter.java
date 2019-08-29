package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.kafkawriter.constant.KafkaPluginErrCode;
import com.alibaba.datax.plugin.writer.kafkawriter.constant.Key;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.RowSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HbKafkaWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();//获取配置文件信息{parameter 里面的参数}
            log.info("kafka writer params:{}", conf.toJSON());
            //校验 参数配置
            this.validateParameter();
        }

        private void validateParameter() {
            this.conf.getNecessaryValue(
                    Key.TOPIC,
                    KafkaPluginErrCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(
                    Key.BOOTSTRAP_SERVERS,
                    KafkaPluginErrCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(
                    Key.JDBCURL,
                    KafkaPluginErrCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(
                    Key.USERNAME,
                    KafkaPluginErrCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(
                    Key.PASSWORD,
                    KafkaPluginErrCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(
                    Key.TABLE,
                    KafkaPluginErrCode.REQUIRED_VALUE);
        }

        @Override
        public void prepare() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            //按照reader 配置文件的格式  来 组织相同个数的writer配置文件
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration splitedTaskConfig = this.conf.clone();
                configurations.add(splitedTaskConfig);
            }
            return configurations;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

    }


    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        private Producer<String, String> producer;

        private Configuration conf;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            producer = new KafkaProducer<>(buildProperties());
        }

        /**
         * 初始化kafka producer配置
         */
        private Properties buildProperties() {
            Properties props = new Properties();
            props.put("bootstrap.servers", conf.getString(Key.BOOTSTRAP_SERVERS));
            props.put("acks", "all");//这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
            props.put("retries", 0);
            // Controls how much bytes sender would wait to batch up before publishing to Kafka.
            //控制发送者在发布到kafka之前等待批处理的字节数。
            //控制发送者在发布到kafka之前等待批处理的字节数。 满足batch.size和ling.ms之一，producer便开始发送消息
            //默认16384   16kb
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            return props;
        }

        /**
         * 开始写入kafka,不指定KafkaConst,使用默认轮训的分区策略
         */
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            log.info("start to writer kafka");
            String username = conf.getString(com.alibaba.datax.plugin.rdbms.writer.Key.USERNAME);
            String password = conf.getString(com.alibaba.datax.plugin.rdbms.writer.Key.PASSWORD);
            String jdbcUrl = conf.getString(com.alibaba.datax.plugin.rdbms.writer.Key.JDBC_URL);
            String table = conf.getString(com.alibaba.datax.plugin.rdbms.writer.Key.TABLE);
            log.info("jdbc: {}, username:{}, password:{}, table:{},", jdbcUrl, username, password, table);
            Connection connection = DBUtil.getConnection(DataBaseType.PostgreSQL, jdbcUrl, username, password);
            String pkName = getFirstPkName(connection, table);
            List<String> tableColumns = DBUtil.getTableColumnsByConn(DataBaseType.PostgreSQL, connection, table, "");
            Record record = null;
            //说明还在读取数据,或者读取的数据没处理完
            while ((record = lineReceiver.getFromReader()) != null) {
                //获取一行数据，按照指定分隔符 拼成字符串 发送出去
                long startTime = System.currentTimeMillis();
                String topic = this.conf.getString(Key.TOPIC);
                Map<String, Object> columnRecords = getColumnRecords(record, tableColumns);
                String pkValue = columnRecords.getOrDefault(pkName, "").toString();
                String producerKey = table + ":" + pkValue;
                String msgValue = new KafkaMsgValue(table, pkName, columnRecords).toJsonString();
                producer.send(new ProducerRecord<>(topic, producerKey, msgValue), new WriterCallback(startTime));
            }
        }

        @Override
        public void destroy() {
            if (producer != null) {
                producer.close();
            }
        }

        private Map<String, Object> getColumnRecords(Record record, List<String> tableColumns) {
            Map<String, Object> columnValues = Maps.newLinkedHashMap();
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return columnValues;
            }
            //record的列数必须和kafka writer配置中的table列数相同
            if (recordLength != tableColumns.size()) {
                throw DataXException.asDataXException(KafkaPluginErrCode.COLUMN_LENGTH, KafkaPluginErrCode.COLUMN_LENGTH.getDescription());
            }
            for (int i = 0; i < recordLength; i++) {
                Column column = record.getColumn(i);
                String columnName = tableColumns.get(i);
                columnValues.put(columnName, column.getRawData());
            }
            return columnValues;
        }

        /**
         * 获取table中第一个主键
         */
        private String getFirstPkName(Connection connection, String table) {
            try {
                ResultSet pkRSet = connection.getMetaData().getPrimaryKeys(null, null, table);
                while (pkRSet.next()) {
                    String columnName = pkRSet.getString("COLUMN_NAME");
                    if (StringUtils.isNotBlank(columnName)) {
                        return columnName;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            throw DataXException.asDataXException(KafkaPluginErrCode.NO_PRIMARY_KEY, KafkaPluginErrCode.NO_PRIMARY_KEY.getDescription());
        }

        static class WriterCallback implements Callback {

            private final Logger logger = LoggerFactory.getLogger(WriterCallback.class);

            private long startTime;

            public WriterCallback(long startTime) {
                this.startTime = startTime;
            }

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("Error sending message to Kafka {} ", exception.getMessage());
                }

                if (logger.isDebugEnabled()) {
                    long eventElapsedTime = System.currentTimeMillis() - startTime;
                    logger.debug("Acked message partition:{} ofset:{}", metadata.partition(), metadata.offset());
                    logger.debug("Elapsed time for send: {}", eventElapsedTime);
                }
            }
        }
    }

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/lee";
        String username = "lee";
        String password = "123456";
        Connection connection = DBUtil.getConnection(DataBaseType.PostgreSQL, jdbcUrl, username, password);
        try {
            ResultSet pkRSet = connection.getMetaData().getPrimaryKeys(null, null, "student");
            while (pkRSet.next()) {
                String columnName = pkRSet.getString("COLUMN_NAME");
                if (StringUtils.isNotBlank(columnName)) {
                    System.out.println(columnName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
