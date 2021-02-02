package com.bigdata.kafka;

import org.apache.flink.configuration.ConfigConstants;

import java.util.Properties;

/**
 * @author wangtan
 * @Date 2021/2/2
 */
public class KafkaConfig {
    public static Properties sourceConfig() {
        Properties kafkaProperties = new Properties();
        //本地机器配置如下
        //这两个的properties是必须传递的
        //以逗号分隔的 Kafka broker 即：消息中间件存在的某个服务器，可能为多个服务器
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        //消费组 ID(自定义名称，不与其他的消费组同名即可)
        //在公司机器上会配置 流对应的消费组
        kafkaProperties.setProperty("group.id", "consumer01");

        //下面的配置是公司里面的配置
        /*Properties properties = PropertiesUtils.load(ConfigConstants.SETTINGS_LOCATION);

        String appId = properties.getProperty(ConfigConstants.APP_ID);
        String passWord = properties.getProperty(ConfigConstants.PASSWORD);
        String clusterId = properties.getProperty(ConfigConstants.CLUSTER_ID);
        String bootstrap = properties.getProperty(ConfigConstants.INPUT_BOOTSTRAP_SERVERS);
        String groupId = properties.getProperty(ConfigConstants.GROUP_ID);
        kafkaProperties.setProperty("bootstrap.servers", bootstrap);
        kafkaProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
        kafkaProperties.setProperty("group.id", groupId);
        kafkaProperties.setProperty("sasl.mechanism", "PLAIN");
        kafkaProperties.setProperty("batch.size", "10000");
        kafkaProperties.setProperty("auto.offset.reset", "latest");
        kafkaProperties.setProperty("enable.auto.commit", "true");
        //请填写正确的clusterId，appId，密码  clusterId对应关系见上表
        String format = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s.%s\" password=\"%s\";";
        String jaas_config = String.format(format, clusterId, appId, passWord);
        kafkaProperties.put("sasl.jaas.config", jaas_config);*/
        return kafkaProperties;
    }

    public static Properties outConfig(){
        Properties out=new Properties();
        out.setProperty("bootstrap.servers", "localhost:9092");
        return out;
    }


}

