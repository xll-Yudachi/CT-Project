package com.yudachi.ct.common.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    /**
     * @Description 将日期字符串按照指定的格式解析为日期对象
     * @Params [dateString, format]
     * @Return java.util.Date
     **/
    public static Date parse(String dateString, String format){

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sdf.parse(dateString);
        }catch (Exception e){
            e.printStackTrace();
        }

        return date;
    }

    public static String format(Date date, String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }
}
