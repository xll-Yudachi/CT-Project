package com.yudachi.producer.io;

import com.yudachi.ct.common.bean.Data;
import com.yudachi.ct.common.bean.DataIn;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author Yudachi
 * @Description 本地文件输入
 * @Date 2019/10/9 12:08
 * @Version 1.0
 **/
public class LocalFileDataIn implements DataIn {

    private BufferedReader br = null;

    public LocalFileDataIn(String path) {
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public Object read() throws Exception {
        return null;
    }

    /**
     * @Description 读取数据 返回集合
     * @Params [clazz]
     * @Return java.util.List<T>
     **/
    @Override
    public <T extends Data> List<T> read(Class<T> clazz) throws Exception {

        List<T> ts = new ArrayList<>();

        try {
            String line = null;
            while ( (line = br.readLine()) != null){
                //将数据转换为指定类型
                T t = clazz.newInstance();
                t.setValue(line);
                ts.add(t);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return ts;
    }

    //关闭资源
    @Override
    public void close() throws IOException {
        if (br!=null) {
            br.close();
        }
    }

}
