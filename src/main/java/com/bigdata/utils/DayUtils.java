package com.bigdata.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wangtan
 * @Date 2021/1/13
 */
public class DayUtils {

    /**
     * @return 当日凌晨的时间戳
     */
    public static Long todayTimeStamp(){
        //当天日期
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String today = simpleDateFormat.format(date.getTime());
        //1、当天凌晨(毫秒)
        long daytime1 = 0;
        try {
            daytime1 = simpleDateFormat.parse(today).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return daytime1;
    }
//    public static Long tomorrowTimeStamp(){
//        //获取当前日期
//        Date date = new Date();
//        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
//        String nowDate = sf.format(date);
//        System.out.println(nowDate);
//        //通过日历获取下一天日期
//        Calendar cal = Calendar.getInstance();
//        try {
//            cal.setTime(sf.parse(nowDate));
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        cal.add(Calendar.DAY_OF_YEAR, +1);
//        String nextDate_1 = sf.format(cal.getTime());
//        System.out.println("明天日期："+nextDate_1);
//       sf.format(date)
//
//    }

}
