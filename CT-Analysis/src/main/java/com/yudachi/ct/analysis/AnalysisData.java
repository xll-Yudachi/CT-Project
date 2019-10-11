package com.yudachi.ct.analysis;

import com.yudachi.ct.analysis.tool.AnalysisBeanTool;
import org.apache.hadoop.util.ToolRunner;

public class AnalysisData {
    public static void main(String[] args) throws Exception {

       // ToolRunner.run(new AnalysisTextTool(),args);

        ToolRunner.run(new AnalysisBeanTool(), args);

    }
}
