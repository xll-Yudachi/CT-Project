package com.yudachi.ct.common.bean;


public abstract class Data implements Val{

    public String content;

    @Override
    public void setValue(Object val) {
        this.content = (String)val;
    }

    @Override
    public String getValue() {
        return content;
    }
}
