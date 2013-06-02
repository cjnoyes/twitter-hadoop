package com.cjnoyessw.twitter.hadoop.utils;

import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

public class DateUtils {

	public DateUtils() {
		calendar = GregorianCalendar.getInstance();
		parse = DateFormat.getDateTimeInstance();
		keyFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	}

	public Date parseDate(String strdate) throws Exception {
		Date date = parse.parse(strdate);
		return date;
	}

	public String formatDate(Date date) {
		return keyFormat.format(date);
	}

	private Calendar calendar;
	private DateFormat parse;
	private SimpleDateFormat keyFormat;
}
