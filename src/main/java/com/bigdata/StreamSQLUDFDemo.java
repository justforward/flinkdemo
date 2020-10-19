package com.bigdata;



import org.apache.flink.table.functions.*;



/**
 * @Auther wangtan
 * @Date 2020/10/15
 */
public class StreamSQLUDFDemo extends ScalarFunction {
    public String eval(String s){
        return s.toUpperCase();
    }
}
