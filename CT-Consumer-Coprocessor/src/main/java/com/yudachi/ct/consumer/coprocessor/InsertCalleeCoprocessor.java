package com.yudachi.ct.consumer.coprocessor;

import com.yudachi.ct.common.bean.BaseDao;
import com.yudachi.ct.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author Yudachi
 * @Description //TODO 协处理器
 * @Date 2019/10/10 15:11
 * @Version 1.0
 *
 * 协处理器的使用：
 * 1. 创建类
 * 2. 让表知道协处理器类（和表有关联）
 **/
public class InsertCalleeCoprocessor extends BaseRegionObserver {

    /**
     * @Description //TODO  保存主叫用户数据之后，由Hbase自动保存被叫用户数据
     * @Params [e, put, edit, durability]
     * @Return void
     **/
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));

        //主叫用户的rowkey
        String rowkey = Bytes.toString(put.getRow());
        String[] values = rowkey.split("_");

        CoprocessorDao coprocessorDao = new CoprocessorDao();

        //保存数据
        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flag = values[5];

        if ("1".equals(flag)){
            String calleeRowkey = coprocessorDao.getRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0" ;


            //被叫用户
            Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));

            table.put(calleePut);

            //关闭表
            table.close();
        }
    }


    private class CoprocessorDao extends BaseDao{
        int getRegionNum(String tel, String time){
            return genRegionNum(tel, time);
        }
    }
}
