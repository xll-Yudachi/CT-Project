package com.yudachi.ct.analysis.io;

import com.yudachi.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLTextOutputFormat extends OutputFormat<Text,Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text>{

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
        public void write(Text key, Text value) throws IOException, InterruptedException {

            String[] values = value.toString().split("_");
            String sumCall = values[0];
            String sumDuration = values[1];

            PreparedStatement pstm = null;

            try {
                String insertSQL = "insert into tb_call (telid, dateid, sumcall, sumduration) values (?,?,?,?)";
                pstm = connection.prepareStatement(insertSQL);

                String string = key.toString();
                System.err.println(string);
                String[] strings = string.split("_");

                String tel = strings[0];
                String date = strings[1];

                System.err.println(date);
                System.err.println(jedis.hget("ct_date", date));

                pstm.setInt(1, Integer.parseInt(jedis.hget("ct_user", tel)));
                pstm.setInt(2, Integer.parseInt(jedis.hget("ct_date", date)));
                pstm.setInt(3, Integer.parseInt(sumCall));
                pstm.setInt(4, Integer.parseInt(sumDuration));
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
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
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
