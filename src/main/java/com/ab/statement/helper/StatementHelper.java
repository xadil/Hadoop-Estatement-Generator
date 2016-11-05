/**
 * 
 */
package com.ab.statement.helper;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author xadil
 *
 */
public class StatementHelper {
	public static final String DEFAULT_DATE_FORMAT = "dd-MM-yyyy HH:mm:ss";
	
	/**
	 * 
	 */
	public StatementHelper() {
		// TODO Auto-generated constructor stub
	}
	
	
	public static String parseDateAndGetLong(String date){
		DateFormat format = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
		try {
			return String.valueOf(format.parse(date).getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	public static String getVeryOldDate(){
		DateFormat format = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
		try {
			return String.valueOf(format.parse("01-01-1900 00:00:00").getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
}
