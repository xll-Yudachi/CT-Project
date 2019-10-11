package com.yudachi.ct.consumer.bean;

import com.yudachi.ct.common.api.Column;
import com.yudachi.ct.common.api.Rowkey;
import com.yudachi.ct.common.api.TableRef;
import lombok.Data;

/**
 * @Author Yudachi
 * @Description //TODO 通话日志对象
 * @Date 2019/10/10 11:35
 * @Version 1.0
 **/
@Data
@TableRef("ct:calllog")
public class Calllog {
    @Column(family = "caller")
    private String call1;
    @Column(family = "caller")
    private String call2;
    @Column(family = "caller")
    private String calltime;
    @Column(family = "caller")
    private String duration;
    @Column(family = "caller")
    private String flag = "1";
    @Rowkey
    private String rowkey;

    public Calllog(){}

    public Calllog(String data){
        String[] values = data.split("\t");
        call1 = values[0];
        call2 = values[1];
        calltime = values[2];
        duration = values[3];
    }
}
