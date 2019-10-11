package com.yudachi.producer.io;

import com.yudachi.ct.common.bean.DataOut;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @Author Yudachi
 * @Description 本地文件输出
 * @Date 2019/10/9 12:09
 * @Version 1.0
 **/
public class LocalFileDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFileDataOut(String path) {
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Object data) throws Exception {
        write(data.toString());
    }

    @Override
    public void write(String data) throws Exception {
        writer.println(data);
        writer.flush();
    }


    @Override
    public void close() throws IOException {
        if(writer != null){
            writer.close();
        }
    }

}
