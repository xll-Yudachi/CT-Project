package com.yudachi.producer;

import com.yudachi.ct.common.bean.Producer;
import com.yudachi.producer.bean.LocalFileProducer;
import com.yudachi.producer.io.LocalFileDataIn;
import com.yudachi.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * @Author Yudachi
 * @Description //启动对象
 * @Date 2019/10/9 12:03
 * @Version 1.0
 **/
public class Bootstrap {
    public static void main(String[] args) throws IOException {

        if(args.length < 2){
            System.out.println("系统参数不正确，请按照指定格式传递： java -jar Produce.jar sourcePath distPath ");
            System.exit(1);
        }

        //构建生产者对象
        Producer producer = new LocalFileProducer();

       /* producer.setIn(new LocalFileDataIn("D:\\IDEAWorkSpace\\CT-Project\\CT-Producer\\src\\main\\java\\com\\yudachi\\producer\\log\\contact.log"));
        producer.setOut(new LocalFileDataOut("D:\\IDEAWorkSpace\\CT-Project\\CT-Producer\\src\\main\\java\\com\\yudachi\\producer\\log\\call.log"));*/


       producer.setIn(new LocalFileDataIn(args[0]));
       producer.setOut(new LocalFileDataOut(args[1]));

        //生产数据
        producer.produce();

        //关闭生产者对象
        producer.close();
    }
}
