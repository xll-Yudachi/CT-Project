package com.yudachi.ct.common.util;

import java.text.DecimalFormat;

/**
 * @Author Yudachi
 * @Description 数字工具类
 * @Date 2019/10/9 13:44
 * @Version 1.0
 **/
public class NumberUtil {

    /**
     * @Description 将数字格式化为字符串
     * @Params [num, length]
     * @Return java.lang.String
     **/
    public static String format(int num, int length){

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append("0");
        }

        DecimalFormat df = new DecimalFormat(builder.toString());

        return df.format(num);
    }
}
