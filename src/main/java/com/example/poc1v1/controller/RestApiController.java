package com.example.poc1v1.controller;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import java.sql.*;

import com.example.poc1v1.model.Data;

import com.github.jkutner.EnvKeyStore;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import com.google.gson.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

@RestController
@RequestMapping("/api")
public class RestApiController {

    public Connection c = null;
    private Logger logger;
    protected Producer<String,String> kafkaProducer;
    protected KafkaConsumer<String,String> kafkaConsumer;


    private class ConsumerThread extends Thread {

        public Statement stmt = null;
        protected KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(KafkaConsumer<String,String> kafkaConsumer) {
            this.kafkaConsumer = kafkaConsumer;
        }

        public void run() {
            try {
                stmt = c.createStatement();
            while(true) {
                String sql;
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + " Value: " + record.value());
                    logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    HashMap<String,String> map = new Gson().fromJson(record.value(), new TypeToken<HashMap<String, String>>(){}.getType());
                    sql = "INSERT INTO customers (firstName, lastName, email, phoneNumber) VALUES ('" +
                            map.get("firstName") + "', '" +
                            map.get("lastName") + "', '" +
                            map.get("email") + "', '" +
                            map.get("phoneNumber") + "');";
                    logger.info(sql);
                    stmt.executeUpdate(sql);
                }
            }

        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }
        }
    }

    RestApiController () {

        logger = LoggerFactory.getLogger(this.getClass());




        try {

            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection(System.getenv("JDBC_DATABASE_URL"));



        } catch (Exception e) {

            e.printStackTrace();

            System.err.println(e.getClass().getName()+": "+e.getMessage());

            System.exit(0);

        }


        try {

            EnvKeyStore envTrustStore = EnvKeyStore.createWithRandomPassword("KAFKA_TRUSTED_CERT");
            EnvKeyStore envKeyStore = EnvKeyStore.createWithRandomPassword("KAFKA_CLIENT_CERT_KEY", "KAFKA_CLIENT_CERT");

            File trustStore = envTrustStore.storeTemp();
            File keyStore = envKeyStore.storeTemp();

            KafkaConfig producerConfig = new KafkaConfig();
            kafkaProducer = new KafkaProducer<>(producerConfig.getKafkaProps(envTrustStore, envKeyStore, trustStore, keyStore));

            ConConfig consumerConfig = new ConConfig();
            kafkaConsumer = new KafkaConsumer<>(consumerConfig.getKafkaProps(envTrustStore, envKeyStore, trustStore, keyStore));

            kafkaConsumer.subscribe(Collections.singleton("cisco"));

            new ConsumerThread(kafkaConsumer).start();

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (KeyStoreException kse) {
            throw new RuntimeException(kse);
        } catch (NoSuchAlgorithmException nsa) {
            throw new RuntimeException(nsa);
        } catch (CertificateException ce) {
            throw new RuntimeException(ce);
        }


    }

    @PostMapping("/send-message")
    public ResponseEntity<Void> sendMessage(@RequestBody Data data) {
        Gson gson = new Gson();
        String jsonString = gson.toJson(data);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("cisco", jsonString);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception != null)
                    exception.printStackTrace();
                else
                    System.out.println("We sent message to Kafka Topic as offset {} " + metadata.offset());
            }
        });
        return null;
    }

}
