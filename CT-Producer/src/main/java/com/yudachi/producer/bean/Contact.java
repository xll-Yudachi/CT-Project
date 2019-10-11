package com.yudachi.producer.bean;

import lombok.Data;
import lombok.ToString;


@Data
@ToString
public class Contact extends com.yudachi.ct.common.bean.Data {
    private String tel;
    private String name;

    @Override
    public void setValue(Object val) {
        content = (String) val;
        String[] values = content.split("\t");
        setName(values[1]);
        setTel(values[0]);
    }
}
