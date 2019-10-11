package com.yudachi.ct.consumer;

import com.yudachi.ct.common.bean.Consumer;
import com.yudachi.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * @Author Yudachi
 * @Description //TODO 启动消费者
 * @Date 2019/10/9 18:36
 * @Version 1.0
 **/
public class Bootstrap {
    public static void main(String[] args) throws IOException {

        // 使用kafka消费者获取Flume采集的数据
        Consumer consumer = new CalllogConsumer();

        //消费数据  将数据存储到Hbase中
        consumer.consume();

        // 关闭资源
        consumer.close();

    }
}
