package com.yudachi.ct.analysis.kv;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description //TODO 自定义分析数据key
 * @Params
 * @Return
 **/
@Data
public class AnalysisKey implements WritableComparable {

    private String tel;
    private String date;

    public AnalysisKey(String tel, String date) {
        this.tel = tel;
        this.date = date;
    }

    public AnalysisKey() {
    }

    /**
     * @Description //TODO 比较
     * @Params [obj]
     * @Return int
     **/
    @Override
    public int compareTo(Object obj) {
        AnalysisKey analysisKey = obj instanceof AnalysisKey ? ((AnalysisKey) obj) : null;

        int result = tel.compareTo(analysisKey.getTel());

        if(result == 0){
            result = date.compareTo(analysisKey.getDate());
        }

        return result;
    }

    /**
     * @Description //TODO 写数据
     * @Params [dataOutput]
     * @Return void
     **/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tel);
        dataOutput.writeUTF(date);
    }

    /**
     * @Description //TODO  读数据
     * @Params [dataInput]
     * @Return void
     **/
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tel = dataInput.readUTF();
        date = dataInput.readUTF();
    }
}
