package com.yudachi.ct.analysis.io;

import com.yudachi.ct.analysis.kv.AnalysisKey;
import com.yudachi.ct.analysis.kv.AnalysisValue;
import com.yudachi.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {

    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue>{

        private Connection connection = null;
        private Jedis jedis = null;

        public MySQLRecordWriter(){
            //获取资源
            connection = JDBCUtil.getConnection();
            //获取redis连接
            jedis = new Jedis("hadoop2", 6379);
        }


        /**
         * @Description //TODO  输出数据
         * @Params [key, value]
         * @Return void
         **/
        @Override
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {

            PreparedStatement pstm = null;

            try {
                String insertSQL = "insert into tb_call (telid, dateid, sumcall, sumduration) values (?,?,?,?)";
                pstm = connection.prepareStatement(insertSQL);

                pstm.setInt(1, Integer.parseInt(jedis.hget("ct_user", key.getTel())));
                pstm.setInt(2, Integer.parseInt(jedis.hget("ct_date", key.getDate())));
                pstm.setInt(3, Integer.parseInt(value.getSumCall()));
                pstm.setInt(4, Integer.parseInt(value.getSumDuration()));
                pstm.executeUpdate();
            }catch (SQLException e){
                e.printStackTrace();
            }finally {
                try {
                    assert pstm != null;
                    pstm.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * @Description //TODO 关闭资源
         * @Params [context]
         * @Return void
         **/
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }


    private FileOutputCommitter committer = null;
    private static Path getOutputPath(JobContext job){
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null){
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
            return committer;
    }
}
