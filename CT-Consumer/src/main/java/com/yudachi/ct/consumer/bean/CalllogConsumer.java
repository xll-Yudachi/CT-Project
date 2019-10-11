package com.yudachi.ct.consumer.bean;

import com.yudachi.ct.common.bean.Consumer;
import com.yudachi.ct.common.constant.Names;
import com.yudachi.ct.consumer.dao.HbaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class CalllogConsumer implements Consumer {

    //消费数据
    @Override
    public void consume() {
        Properties properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));

            // 获取flume采集的数据
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

            //关注主题
            kafkaConsumer.subscribe(Collections.singletonList(Names.TOPIC.getValue()));

            HbaseDao hbaseDao = new HbaseDao();
            hbaseDao.init();

            //消费数据
            while(true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.err.println(consumerRecord.value());
                    hbaseDao.insertData(consumerRecord.value());
                    //Calllog calllog = new Calllog(consumerRecord.value());
                    //hbaseDao.insertData(calllog);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    //关闭资源
    @Override
    public void close() throws IOException {

    }
}
