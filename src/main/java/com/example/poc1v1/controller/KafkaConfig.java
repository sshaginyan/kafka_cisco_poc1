package com.example.poc1v1.controller;

import com.github.jkutner.EnvKeyStore;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Properties;

public class KafkaConfig {
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

                        props.put(ProducerConfig.ACKS_CONFIG, "all");
                        props.put(ProducerConfig.RETRIES_CONFIG, 0);
                        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
                        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
                        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
                        // configured to use Strings as key and value
                        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                        //try {

//                            EnvKeyStore envTrustStore = EnvKeyStore.createWithRandomPassword("KAFKA_TRUSTED_CERT");
//                            EnvKeyStore envKeyStore = EnvKeyStore.createWithRandomPassword("KAFKA_CLIENT_CERT_KEY", "KAFKA_CLIENT_CERT");
//
//                            File trustStore = envTrustStore.storeTemp();
//                            File keyStore = envKeyStore.storeTemp();

                            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envTrustStore.type());
                            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.getAbsolutePath());
                            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, envTrustStore.password());

                            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envKeyStore.type());
                            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore.getAbsolutePath());
                            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, envKeyStore.password());

                            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");


//                        } catch (IOException ioe) {
//                            throw new RuntimeException(ioe);
//                        } catch (KeyStoreException kse) {
//                            throw new RuntimeException(kse);
//                        } catch (NoSuchAlgorithmException nsa) {
//                            throw new RuntimeException(nsa);
//                        } catch (CertificateException ce) {
//                            throw new RuntimeException(ce);
//                        }
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
