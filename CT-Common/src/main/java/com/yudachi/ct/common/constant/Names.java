package com.yudachi.ct.common.constant;

import com.yudachi.ct.common.bean.Val;

public enum Names implements Val {

    NAMESPACE("ct"),TOPIC("ct"),TABLE("ct:calllog"),CF_CALLER("caller"),CF_CALLEE("callee");


    private String name;

    Names(String name) {
        this.name = name;
    }


    @Override
    public void setValue(Object val) {
        this.name = (String) val;
    }

    @Override
    public String getValue() {
        return name;
    }
}
