package com.yudachi.ct.common.bean;

import java.io.Closeable;

/**
 * @Author Yudachi
 * @Description //生产者接口
 * @Date 2019/10/9 11:48
 * @Version 1.0
 **/
public interface Producer extends Closeable {

    public void setIn(DataIn in);

    public void setOut(DataOut out);

    public void produce();

}
