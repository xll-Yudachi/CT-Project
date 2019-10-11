package com.yudachi.ct.common.bean;

import java.io.Closeable;
import java.util.List;

public interface DataIn extends Closeable {
    public void setPath(String path);

    public Object read() throws Exception;

    public <T extends Data> List<T> read(Class<T> clazz) throws Exception;
}
