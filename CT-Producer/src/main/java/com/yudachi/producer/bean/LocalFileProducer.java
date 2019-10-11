package com.yudachi.producer.bean;

import com.yudachi.ct.common.bean.DataIn;
import com.yudachi.ct.common.bean.DataOut;
import com.yudachi.ct.common.bean.Producer;
import com.yudachi.ct.common.util.DateUtil;
import com.yudachi.ct.common.util.NumberUtil;
import lombok.Data;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @Author Yudachi
 * @Description //本地数据文件生产者
 * @Date 2019/10/9 12:04
 * @Version 1.0
 **/

@Data
public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;
    //增强可见性 外部修改值 这里可以看见
    private volatile boolean flag = true;

    //生产数据
    @Override
    public void produce() {
       try {
           //读取通讯录数据
           List<Contact> contacts = in.read(Contact.class);
           //每秒生成2条数据
           while (true){

               //从通讯录中随机查找2个电话号码(主叫，被叫)
               int call1Index = new Random().nextInt(contacts.size());
               int call2Index;

               while(true){
                   call2Index = new Random().nextInt(contacts.size());
                   if (call1Index!=call2Index) {
                       break;
                   }
               }

               Contact call1 = contacts.get(call1Index);
               Contact call2 = contacts.get(call2Index);

               //生成随机的通话时间
               String startDate = "20190101000000";
               String endDate = "20200101000000";

               long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
               long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

               //通话时间
               long calltime = startTime + (long)((endTime - startTime) * Math.random());
               //通话时间字符串
               String callTimeString = DateUtil.format(new Date(calltime), "yyyyMMddHHmmss");

               //生成随机的通话时长
               String duration = NumberUtil.format(new Random().nextInt(3000), 4);

               //生成通话记录
               Calllog calllog = new Calllog(call1.getTel(), call2.getTel(), callTimeString, duration);

               System.err.println(calllog.toString());

               //将通话记录刷写到数据文件中
               out.write(calllog);

               Thread.sleep(500);
           }
       }catch (Exception e){
           e.printStackTrace();
       }
    }

    //关闭生产者
    @Override
    public void close() throws IOException {
        if(in != null){
            in.close();
        }
        if(out != null){
            out.close();
        }
    }

}
