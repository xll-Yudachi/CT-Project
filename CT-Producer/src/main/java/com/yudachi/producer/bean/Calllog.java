package com.yudachi.producer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Calllog {
    private String call1;
    private String call2;
    private String calltime;
    private String duration;

    @Override
    public String toString() {
        return call1 + "\t" + call2 + "\t" + calltime + "\t" + duration;
    }
}
