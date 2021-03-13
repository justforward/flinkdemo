package com.bigdata.source.output;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wangtan
 * @Date 2021/3/12
 */
public class HDFSSinkFunction extends RichSinkFunction<String> {

    private FileSystem fs = null;
    private SimpleDateFormat sf = null;
    /**
     * 按照时间切割
     */
    private String pathStr = null;


    /**
     * 第一次执行
     * @param parameters  flink传入的configuration
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //该Configuration是hadoop下面的
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        fs = FileSystem.get(conf);
        sf = new SimpleDateFormat("yyyyMMddHH");
        //hdfs的地址
        pathStr = "hdfs://ns1/user/qingniu/flinkstreaminghdfs";
    }

    /**
     * 关闭
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        fs.close();
    }

    /**
     * 具体的执行，上游给一条该方法就被调用一次！与source的run方法不同，source的run需要自己控制循环
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        if (null != value) {
            String format = sf.format(new Date());
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            StringBuilder sb = new StringBuilder();
            sb.append(pathStr).append("/").append(indexOfThisSubtask).append("_").append(format);
            Path path = new Path(sb.toString());
            FSDataOutputStream fsd = null;
            if (fs.exists(path)) {
                fsd = fs.append(path);
            } else {
                fsd = fs.create(path);
            }

            fsd.write((value + "\n").getBytes("UTF-8"));
            fsd.close();
        }
    }



}
