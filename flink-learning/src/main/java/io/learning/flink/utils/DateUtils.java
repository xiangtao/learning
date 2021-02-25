package io.learning.flink.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author taox
 * Created by taox on 2019/6/25.
 */
public class DateUtils {

	public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss";
	public static final String PATTERN_YMDHMS_ONE = "yyyy-MM-dd HH:mm:ss";

	public DateUtils() {
	}

	public static Date now() {
		return new Date(System.currentTimeMillis());
	}

	public static Date getDate(int year, int month) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month - 1, 1);
		return calendar.getTime();
	}

	public static Date getDate(int year, int month, int day) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month - 1, day);
		return calendar.getTime();
	}

	public static String format(Date date) {
		return format(date, "yyyy/MM/dd HH:mm:ss");
	}

	public static String format(Date date, String pattern) {
		if (date == null) {
			return "";
		} else {
			SimpleDateFormat format = new SimpleDateFormat(pattern);
			return format.format(date);
		}
	}

	public static Date parse(String date) {
		return parse(date, "yyyy/MM/dd HH:mm:ss");
	}

	public static Date parse(String date, String pattern) {
		SimpleDateFormat format = new SimpleDateFormat(pattern);

		try {
			return format.parse(date);
		} catch (Exception var4) {
			return null;
		}
	}

	public static Date addMinute(Date date, int minutes) {

		if (date == null) {
			return null;
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MINUTE, minutes);

		return calendar.getTime();
	}

	public static Date addHour(Date date, int hours) {

		if (date == null) {
			return null;
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.HOUR_OF_DAY, hours);

		return calendar.getTime();
	}

	public static Date addDay(Date date, int days) {

		if (date == null) {
			return null;
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, days);

		return calendar.getTime();
	}

	public static Date addMonth(Date date, int months) {
		if (date == null) {
			return null;
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, months);

		return calendar.getTime();
	}

	public static Date addYear(Date date, int years) {
		if (date == null) {
			return null;
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.YEAR, years);

		return calendar.getTime();
	}

	public static int dayInterval(Date startDate, Date endDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(startDate);
		long startTime = calendar.getTimeInMillis();
		calendar.setTime(endDate);
		long endTime = calendar.getTimeInMillis();
		long interval = Math.abs(endTime - startTime) / (1000L * 60 * 60 * 24);

		return Integer.parseInt(String.valueOf(interval));
	}
}
