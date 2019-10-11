package com.yudachi.ct.common.bean;

import java.io.Closeable;

/*
 * @Author Yudachi
 * @Description // TODO 消费者接口
 * @Date 2019/10/9 18:38
 * @Version 1.0
 **/
public interface Consumer extends Closeable {

    //消费数据
    public void consume();
}
