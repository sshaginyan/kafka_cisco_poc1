package com.example.poc1v1.controller;

import com.github.jkutner.EnvKeyStore;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Properties;

public class ConConfig {
    private Logger logger;

    public Properties getKafkaProps(EnvKeyStore envTrustStore, EnvKeyStore envKeyStore, File trustStore, File keyStore) {

        logger = LoggerFactory.getLogger(this.getClass());

        Properties props = new Properties();
        StringBuilder builder = new StringBuilder();

        for(String url : System.getenv("KAFKA_URL").split(",")) {
            try {
                URI uri = new URI(url);
                builder.append(String.format("%s:%d", uri.getHost(), uri.getPort()));
                builder.append(',');

                switch (uri.getScheme()) {
                    case "kafka" :
                        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
                        break;
                    case "kafka+ssl" :
                        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
                        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "cisco");
                        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envTrustStore.type());
                        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.getAbsolutePath());
                        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, envTrustStore.password());

                        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envKeyStore.type());
                        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore.getAbsolutePath());
                        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, envKeyStore.password());

                        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

                        break;
                }

            } catch (URISyntaxException se) {
                throw new RuntimeException(se);
            }
        }
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, builder.toString().substring(0,builder.toString().length()-1));

        logProps(props);

        return props;
    }

    private void logProps(Properties props) {
        @SuppressWarnings("unchecked")
        Enumeration<String> en = (Enumeration<String>)props.propertyNames();
        while(en.hasMoreElements()) {
            String propName = en.nextElement();
            logger.info("[{}] => [{}]", propName, props.get(propName));
        }
    }
}
