package com.bigdata.source.input;

import com.google.gson.internal.$Gson$Preconditions;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 * @author wangtan
 * @Date 2021/3/11
 */
public class FileCountryDictSourceFunction implements SourceFunction<String> {

    private Boolean isCancel = true;
    private Integer interval=10000;

    /**
     * 创建要往下输出的数据，并且发送出去
     *
     * @param ctx
     * @throws Exception 读取一个文件，如果这个文件有变动，那么就重复的把文件读进来，进行往下发送
     *                   如果这个文件没有发生变动，就不往下发送。
     *                   <p>
     *                   注意：flink是不会主动循环调用run方法的，得自己控制什么时候退出
     *                    该代码中，当摁下取消键的时候，就会退出
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //从hadoop读取文件，存放到本地
        Path path = new Path("/Users/didi/hdfs/input/txt/");
        FileSystem fileSystem = FileSystem.get(new Configuration());
        String md5=null;
        while (isCancel) {
            if(!fileSystem.exists(path)){
                Thread.sleep(interval);
                continue;
            }
            //得到文件的md5
            FileChecksum fileChecksum = fileSystem.getFileChecksum(path);
            String md5Str = fileChecksum.toString();
            String currentMD5 = md5Str.substring(md5Str.indexOf(":") + 1);
           if(!currentMD5.equals(md5)){
               //文件有变动，执行文件读取
               FSDataInputStream open = fileSystem.open(path);
               BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open));
               String line = bufferedReader.readLine();
               while(line!=null){
                   //从文件中每次读取一行数据，然后往下发送字符串
                   ctx.collect(line);
                   line=bufferedReader.readLine();
               }
               //读完关闭
               bufferedReader.close();
               md5=currentMD5;
           }
           Thread.sleep(interval);
        }
    }

    /**
     * 取消source发送
     */
    @Override
    public void cancel() {
        isCancel = false;
    }
}
