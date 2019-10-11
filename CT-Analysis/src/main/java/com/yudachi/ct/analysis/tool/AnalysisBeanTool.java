package com.yudachi.ct.analysis.tool;

import com.yudachi.ct.analysis.io.MySQLBeanOutputFormat;
import com.yudachi.ct.analysis.kv.AnalysisKey;
import com.yudachi.ct.analysis.kv.AnalysisValue;
import com.yudachi.ct.analysis.mapper.AnalysisBeanMapper;
import com.yudachi.ct.analysis.reducer.AnalysisBeanReducer;
import com.yudachi.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * @Author Yudachi
 * @Description //TODO  分析数据工具类
 * @Date 2019/10/10 16:58
 * @Version 1.0
 **/
public class AnalysisBeanTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisBeanTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        //mapper
        TableMapReduceUtil.initTableMapperJob(Names.TABLE.getValue(), scan, AnalysisBeanMapper.class, AnalysisKey.class, Text.class, job);

        //reducer
        job.setReducerClass(AnalysisBeanReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        //outputformat
        job.setOutputFormatClass(MySQLBeanOutputFormat.class);

        boolean flag = job.waitForCompletion(true);

        if(flag){
            return JobStatus.State.SUCCEEDED.getValue();
        }else{
            return JobStatus.State.FAILED.getValue();
        }
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
