package com.alibaba.middleware.race;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class TimeUtil {
	public static String getDataByMin(long TimeMil) {
		Calendar calendar=new GregorianCalendar();
		calendar.setTimeInMillis(TimeMil);
		calendar.set(calendar.get(Calendar.YEAR),
				calendar.get(Calendar.MONTH),
				calendar.get(Calendar.DAY_OF_MONTH),
				calendar.get(Calendar.HOUR),
				calendar.get(Calendar.MINUTE),0);
		long mil=calendar.getTimeInMillis();
		return String.valueOf(mil).substring(0, 10);
	}
	public static String getStrMinuteTime(long TimeMil){
		long minuteTime=(TimeMil/1000/60)*60;
		return String.valueOf(minuteTime).substring(0, 10);
		
	}
	public static long getMinuteTime(long TimeMil){
		long minuteTime=(TimeMil/1000/60)*60;
		return minuteTime;
		
	}
}
