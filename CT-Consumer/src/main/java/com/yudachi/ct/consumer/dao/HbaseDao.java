package com.yudachi.ct.consumer.dao;

import com.yudachi.ct.common.bean.BaseDao;
import com.yudachi.ct.common.constant.Names;
import com.yudachi.ct.common.constant.ValueConstant;
import com.yudachi.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDao extends BaseDao {

    /**
     * @Description //TODO 初始化
     * @Params []
     * @Return void
     **/
    public void init() throws IOException {
        start();

        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(), "com.yudachi.ct.consumer.coprocessor.InsertCalleeCoprocessor",ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue(), Names.CF_CALLEE.getValue());

        stop();
    }

    /**
     * @Description //TODO  插入数据
     * @Params [value]
     * @Return void
     **/
    public void insertData(String value) throws IOException {
        //将通话日志保存到Hbase表中

        //1.获取通话日志的数据
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        //2.创建数据对象
        String rowkey = genRegionNum(call1, calltime) + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";

        //主叫用户
        Put put = new Put(Bytes.toBytes(rowkey));

        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());

        put.addColumn(family, Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(family, Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(family, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        put.addColumn(family, Bytes.toBytes("flag"), Bytes.toBytes("1"));


        /*//被叫用户
        String calleeRowkey = genRegionNum(call2,calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";
        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));*/


        //3. 保存数据
        List<Put> puts = new ArrayList<>();
        puts.add(put);
        //puts.add(calleePut);

        putData(Names.TABLE.getValue(), puts);

    }

    public void insertData(Calllog calllog) throws IOException, IllegalAccessException {
        calllog.setRowkey(genRegionNum(calllog.getCall1(), calllog.getCalltime()) + "_" + calllog.getCall1() + "_" + calllog.getCalltime() + "_" + calllog.getCall2() + "_" + calllog.getDuration());

        putData(calllog);
    }

}
















