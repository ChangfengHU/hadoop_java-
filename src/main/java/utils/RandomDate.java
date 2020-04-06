package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RandomDate {
    public static void main(String[] args) {

        for (int i=0;i<30;i++){
            Date date = randomDate("2019-01-01","2019-02-31");
            System.out.println(new SimpleDateFormat("yyyy.MM.dd").format(date));
        }
    }
    public static Date randomDate(String beginDate, String endDate){
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);

            if(start.getTime() >= end.getTime()){
                return null;
            }
            long date = random(start.getTime(),end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static long random(long begin,long end){
        long rtn = begin + (long)(Math.random() * (end - begin));
        if(rtn == begin || rtn == end){
            return random(begin,end);
        }
        return rtn;
    }
    public static long randomLong(long max,long min){
        long i = (long) (min + Math.random() * (max - min + 1));
        return i;
    }



}
