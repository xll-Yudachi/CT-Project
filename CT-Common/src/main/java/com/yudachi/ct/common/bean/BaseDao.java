package com.yudachi.ct.common.bean;

import com.yudachi.ct.common.api.Column;
import com.yudachi.ct.common.api.Rowkey;
import com.yudachi.ct.common.api.TableRef;
import com.yudachi.ct.common.constant.ValueConstant;
import com.yudachi.ct.common.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @Author Yudachi
 * @Description //TODO  基础数据访问对象
 * @Date 2019/10/9 19:07
 * @Version 1.0
 **/
public abstract class BaseDao {

    private ThreadLocal<Connection > connHolder = new ThreadLocal<>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<>();

    protected void start() throws IOException{
        getConnection();
        getAdmin();
    }

    protected void stop() throws IOException {
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }

        Connection connection = getConnection();
        if (connection != null) {
            connection.close();
            connHolder.remove();
        }

    }

    protected void createTableXX(String name,String coprocessorClass ,String... families) throws IOException {
        createTableXX(name, coprocessorClass,null, families);
    }

    /**
     * @Description //TODO 创建表，如果表已经存在，那么删除后再创建新的
     * @Params [name, families]
     * @Return void
     **/
    protected void createTableXX(String name, String coprocessorClass ,Integer regionCount, String ... families) throws IOException {
        Admin admin = getAdmin();

        TableName tableName = TableName.valueOf(name);

        if(admin.tableExists(tableName)){
            deleteTable(name);
        }

        //创建表
        createTable(name, coprocessorClass, regionCount, families);

    }

    /**
     * @Description //TODO  创建命名空间，如果命名空间已经存在，不需要创建，否则创建新的
     * @Params [namespace]
     * @Return void
     **/
    protected void createNamespaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();

        try {
            admin.getNamespaceDescriptor(namespace);
        }catch (NamespaceNotFoundException e){
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * @Description //TODO 删除表
     * @Params [name]
     * @Return void
     **/
    protected void deleteTable(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * @Description //TODO  创建表
     * @Params [name, families]
     * @Return void
     **/
    private void createTable(String name, String coprocessorClass, Integer regionCount, String... families) throws IOException {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        if (families == null || families.length == 0) {
            families = new String[1];
            families[0] = "info";
        }

        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }

        if (coprocessorClass != null && !"".equals(coprocessorClass)) {
            tableDescriptor.addCoprocessor(coprocessorClass);
        }

        //增加预分区
        if (regionCount == null || regionCount <= 1) {
            admin.createTable(tableDescriptor);
        }else{
            //分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }
    }

    /**
     * @Description //TODO 获取HBase的连接对象
     * @Params []
     * @Return org.apache.hadoop.hbase.client.Connection
     **/
    protected Connection getConnection() throws IOException {
        Connection connection = connHolder.get();
        if (connection == null) {
            Configuration configuration = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(configuration);
            connHolder.set(connection);
        }

        return connection;
    }


    /**
     * @Description //TODO 获取管理对象
     * @Params []
     * @Return org.apache.hadoop.hbase.client.Admin
     **/
    protected Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }

        return admin;
    }

    /**
     * @Description //TODO 生成分区键
     * @Params [regionCount]
     * @Return byte[][]
     **/
    private byte[][] genSplitKeys(int regionCount){

        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        List<byte[]> bsList = new ArrayList<>();
        for (int i = 0; i <splitKeyCount ; i++) {
            String splitkey = i + "|";
            bsList.add(Bytes.toBytes(splitkey));
        }

        bsList.toArray(bs);

        return bs;
    }

    /**
     * @Description //TODO  生成分区值
     * @Params [tel, date]
     * @Return int
     **/
    protected int genRegionNum(String tel, String date){

        String usercode = tel.substring(tel.length() - 4);
        String yearMonth = date.substring(0,6);

        int userCodeHash =  usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        //crc校验采用异或算法, hash
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        //取模满足分区数
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;

    }

    /**
     * @Description //TODO  增加多条数据
     * @Params [name, puts]
     * @Return void
     **/
    protected void putData(String name, List<Put> puts ) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));

        table.put(puts);

        table.close();
    }

    /**
     * @Description //TODO  增加数据
     * @Params [name, put]
     * @Return void
     **/
    protected void putData(String name, Put put) throws IOException {
        //获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));

        //增加数据
        table.put(put);

        //关闭表
        table.close();
    }

    /**
     * @Description //TODO 增加对象 自动封装数据， 将对象直接保存到Hbase中去
     * @Params [obj]
     * @Return void
     **/
    protected void putData(Object obj) throws IOException, IllegalAccessException {

        //反射
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();
        String rowkeyString = "";

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            Rowkey rowkey = field.getAnnotation(Rowkey.class);
            if (rowkey != null) {
                field.setAccessible(true);
                rowkeyString = (String)field.get(obj);
                break;
            }
        }

        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkeyString));


        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            if (column!=null) {
                String family = column.family();
                String colName = column.column();
                if (colName == null || "".equals(colName)) {
                    colName = field.getName();
                }
                field.setAccessible(true);
                String value = (String) field.get(obj);

                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
            }
        }

        //增加数据
        table.put(put);

        //关闭表
        table.close();
    }

    /**
     * @Description //TODO 获取查询时startrow, stoprow集合
     * @Params [tel, start, end]
     * @Return java.util.List<java.lang.String[]>
     **/
    protected List<String[]> getStartStoreRowkeys(String tel, String start, String end){

        List<String[]> rowkeysList = new ArrayList<>();

        String startTime = start.substring(0,6);
        String endTime = end.substring(0,6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime, "yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime, "yyyyMM"));

        while(startCal.getTimeInMillis() <= endCal.getTimeInMillis()){
            // 当前时间
            String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");

            int regionNum = genRegionNum(tel, nowTime);

            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String stopRow = startRow + "|";

            String[] rowkeys = {startRow,stopRow};
            rowkeysList.add(rowkeys);

            startCal.add(Calendar.MONTH, 1);
        }
            return rowkeysList;
    }
}















