package com.yudachi.ct.analysis.kv;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class AnalysisValue implements Writable {

    private String sumCall;
    private String sumDuration;

    public AnalysisValue() {
    }

    public AnalysisValue(String sumCall, String sumDuration) {
        this.sumCall = sumCall;
        this.sumDuration = sumDuration;
    }


    /**
     * @Description //TODO  写数据
     * @Params [dataOutput]
     * @Return void
     **/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(sumCall);
        dataOutput.writeUTF(sumDuration);
    }

    /**
     * @Description //TODO 读数据
     * @Params [dataInput]
     * @Return void
     **/
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sumCall = dataInput.readUTF();
        sumDuration = dataInput.readUTF();
    }
}
