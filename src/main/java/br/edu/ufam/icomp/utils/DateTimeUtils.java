package br.edu.ufam.icomp.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import javax.xml.bind.DatatypeConverter;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

public class DateTimeUtils {
	
	private static Logger logger = Logger.getLogger(DateTimeUtils.class);
	
    //<editor-fold defaultstate="collapsed" desc=" Constants ">

    public static final String DF = "yyyy-MM-dd'T'HH:mm:ssZ";

    /** Date format used to compose folder names */
    public static final String DF_FOLDER_NAME = "yyyyMMdd";
    
    /** Date format used to compose folder names */
    //public static String DF_FULL_FOLDER_NAME = "yyyyMMdd-ssmmHH";
    public static String DF_FULL_FOLDER_NAME = "yyyyMMdd-HHmm";

    private static final int FULL_DATETIME_LENGTH = "2011-06-22T07:57:30-04:00".length();
    private static final int TIMEZONE_LENGTH      = "-04:00".length();

    // The month definition is not Java compatible. Java months start in 0;
    public static final int EPOCH_START_MONTH         = 1;
    public static final String EPOCH_START_MONTH_NAME = "January";
    public static final int EPOCH_START_DAY           = 1;
    public static final int EPOCH_START_YEAR          = 1970;

    private static enum Operation {
        ADD,
        SUB;
    };

    //</editor-fold>
    
    //<editor-fold defaultstate="collapsed" desc=" Public Methods ">

    public static double getDuration(Date ultimo, Date primeiro, int scale) {
        double durationInMillis = ultimo.getTime() - primeiro.getTime();
        switch (scale) {
            case Calendar.MINUTE:
                return durationInMillis / 60000;
            case Calendar.MILLISECOND:
                return durationInMillis;
            case Calendar.SECOND:
                return durationInMillis / 1000;
            case Calendar.HOUR:
                return durationInMillis / 3600000;
        }
        throw new IllegalArgumentException("invalid scale specified");
    }
    
    public static String AddDay(String dt, int totalDays) {
    	try{
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		    Calendar c = Calendar.getInstance();
		    c.setTime(sdf.parse(dt));
		    c.add(Calendar.DATE, totalDays); 		// number of days to add
		    dt = sdf.format(c.getTime()); 		 	// dt is now the new date
		    return dt;
    	}catch(ParseException e){
    		logger.error("Not possible increment date. " + e.getMessage());
    	}
    	return null;
    }
    
    public static Date formatStringtoDate(String d) {
    	return formatStringtoDate(d, null);
    }
    
    public static Date formatStringtoDate(String d, String format) {
    	if (StringHelper.isNullOrEmpty(format))
    		format = "yyyy-MM-dd";
    	
    	String[] tmp = d.split(" ");
    	if(tmp.length > 1){
    		format = "yyyy-MM-dd' 'HH:mm:ss";
    	}
    	
    	try{
    		Date date = new SimpleDateFormat(format).parse(d);     
			return date;
    	}catch(Exception ex){
    		logger.error("Parameters: " + d + " and " + format + " .Error: "+ ex.getMessage());
    		 //Logger.getLogger(DateTimeUtils.class.getName()).log(Level.SEVERE, "Could not parse "+d, ex);
    	}
    	return null;
    }

    /**
     * Formats a specified date for use in the DB
     *
     * @param date date that should be formatted
     * @return date in string form formatted with yyyy-MM-dd' 'HH:mm:ss
     *
     */
    public static String formatDateToDBString(Date date) {
        String result = null;

        // DB string datetime format
        String sdf = "yyyy-MM-dd' 'HH:mm:ss";

        result = new SimpleDateFormat(sdf).format(date);

        return result;
    }

    /**
     * Formats a specified date for use in web services
     *
     * @param date date that should be formatted
     * @return date formatted with yyyy-MM-dd'T'H:m:sZ mask
     *
     */
    public static String formatDateToString(Date date) {

        String value = null;
        GregorianCalendar c = new GregorianCalendar();
        
        try {
            c.setTime(date);

            XMLGregorianCalendar newDate =
                    DatatypeFactory.newInstance().newXMLGregorianCalendar(c);

            newDate.setFractionalSecond(null);

            value = newDate.toXMLFormat();

        } catch (DatatypeConfigurationException ex) {
        	logger.error("Parameter " + date.toString() + ". Error: " + ex.getMessage());
            //Logger.getLogger(DateTimeUtils.class.getName()).log(Level.FINE, null, ex);
        }

        return value;
    }
    
    /**
     * Formats a specified date for use in web services
     *
     * @param date date that should be formatted
     * @return date formatted with format mask
     *
     */
    public static String formatDateToString(Date date, String format) {

        String value = null;
        
        try {
        	value = new SimpleDateFormat(format).format(date);

        } catch (Exception ex) {
        	if (date == null)
        		logger.error("Parameters: null format " + format + " .Error: " + ex.getMessage());
        	else
        		logger.error("Parameters: " + date.toString() + " format " + format + " .Error: " + ex.getMessage());
            //Logger.getLogger(DateTimeUtils.class.getName()).log(Level.FINE, null, ex);
        }

        return value;
    }
    
    /**
     * Formats a specified date for use in folder names.
     * 
     * @param date date that should be formatted
     * @return date formatted
     *
     */
    public static String formatDateToFolderName(Date date) {
        return new SimpleDateFormat(DF_FOLDER_NAME).format(date);
    }
    
    /**
     * Formats a specified date for use in folder names.
     * 
     * @param date date that should be formatted
     * @return date formatted in full date yyyyMMdd-ssmmHH
     * The dates are inverted due sort order
     *
     */
    public static String formatFullDateToFolderName(Date date) {
        return new SimpleDateFormat(DF_FULL_FOLDER_NAME).format(date);
    }
    
    public static String formatFullDateToString(Date date, String format) {
        return new SimpleDateFormat(format).format(date);
    }

    public static Date addMinutes(Date sourceDate, int minutes) {

        return changeDate(sourceDate, Operation.ADD, Calendar.MINUTE, minutes);
    }

    private static Date changeDate(Date sourceDate, Operation op, int type,
            int timeframe) {

        Date resultDate = null;

        Calendar calendarData = Calendar.getInstance();
        calendarData.setTime(sourceDate);

        if (op == null) {
            throw new IllegalArgumentException("Null operation.");
        } else if (type != Calendar.MINUTE && type != Calendar.HOUR) {
            throw new IllegalArgumentException("Untested date arithmetic for this type.");
        } else if (op.equals(Operation.ADD)) {

            calendarData.add(type, timeframe);
            resultDate = calendarData.getTime();
        } else if (op.equals(Operation.SUB)) {

            calendarData.add(type, -1 * timeframe);
            resultDate = calendarData.getTime();
            
        } else {
            throw new IllegalArgumentException("Operation not implemented.");
        }

        return resultDate;
    }

    public static Date subtractHours(Date sourceDate, int hours) {
        return changeDate(sourceDate, Operation.SUB, Calendar.HOUR, hours);
    }

    /**
     * Parses a specified date string, returning a Java Date. Only works for
     * simple dates, if passing datetime it fails.
     *
     * @param date - string representing the date (only yyyy-MM-dd).
     * @return date as a Java object.
     *
     */
//    public static Date parseDateString(String str) {
//
//        Date date = null;
//
//        String dateStringFormat = "yyyy-MM-dd";
//
//        try {
//
//            if ((str != null) && (str.length() <= dateStringFormat.length())) {
//                date = new SimpleDateFormat(dateStringFormat).parse(str);
//            }
//        } catch (ParseException ex) {
//            Logger.getLogger(DateTimeUtils.class.getName()).log(Level.FINE, null, ex);
//        }
//
//        return date;
//    }
    
    /**
     * Parses a specified date string, returning a Java Date. Only works for
     * simple dates, if passing datetime it fails.
     *
     * @param date - string representing the date (only yyyy MM dd).
     * @return date as a Java object.
     *
     */
    public static Date parseDateString(String str, String format) {
    	Date date = null;

        try {

            if ((str != null) && (str.length() <= format.length())) {
                date = new SimpleDateFormat(format, Locale.US	).parse(str);
            }
        } catch (ParseException ex) {
        	logger.error("Parameters " + str + " foramt " + format + " .Error: " + ex.getMessage());
            //Logger.getLogger(DateTimeUtils.class.getName()).log(Level.FINE, null, ex);
        }

        return date;
    	
    }

    /**
     * Parses a specified datetime string into a java Date.
     *
     * @param datetimeString - string representing the date in full W3C form
     * @return java date
     *
     * @since v 1.01.00
     */
    public static Date parseDateTimeString(String datetimeString) {

        Date date = null;

        if (datetimeString != null) {
            Calendar c = DatatypeConverter.parseDateTime(datetimeString);
            date = c.getTime();
        }

        return date;
    }

    /**
     * Checks if a given string is a valid datetime in full W3C form.
     *
     * @param datetime - string to check.
     * @return true or false
     *
     * @since v 1.02.05
     */
    public static Boolean isValidFullDateTime(String datetime) {

        Boolean result = false;

        try {

            if (!StringHelper.isNullOrEmpty(datetime)) {
                Calendar c = DatatypeConverter.parseDateTime(datetime);
                Date date = c.getTime();

                if (date != null) {
                    result = true;
                }

            }
        } catch (Exception e) {
            logger.error("Parameter " + datetime + " .Error: " + e.getMessage());
        }

        return result;
    }

    /**
     * Parses a specified datetime string for its day of the week.
     *
     * @param date - string representing the date in full W3C form
     * @return day of week in three letter form
     *
     * @since v 1.01.00
     */
    public static String getDayOfWeek(String datetime){

        String dayOfWeek = null;

        Date d = parseDateTimeString(datetime);

        dayOfWeek = new SimpleDateFormat("EEE").format(d);

        return dayOfWeek;
    }

    /**
     * Recreates a DateTimeZone from a given string.
     *
     * @param datetimeStr - string representing the full datetime in W3C form.
     * @return DateTimeZone object
     *
     * @since v 1.02.00
     */
    public static DateTimeZone getTimeZoneFromFullString(String datetimeStr) {

        String tz = DateTimeUtils.getTimeZoneString(datetimeStr);
        return getTimeZoneFromTZString( tz );
    }

    /**
     * Recreates a DateTimeZone from a given string.
     *
     * @param tzStr - string representing the time zone (e.g. "-11:30").
     * @return DateTimeZone object
     *
     * @since v 1.02.00
     */
    public static DateTimeZone getTimeZoneFromTZString(String tzStr) {

        DateTimeZone originalTZ = null;

        // Timezone string has the form: -03:30
        if (isTimeZoneString(tzStr)) {

            String[] parts = tzStr.split(":");

            // Replace to avoid the problem of parsing integer with + sign.
            int h = Integer.parseInt(parts[0].replaceAll("\\+", ""));
            int m = Integer.parseInt(parts[1]);

            originalTZ = DateTimeZone.forOffsetHoursMinutes(h, m);
        }
        
        return originalTZ;
    }

    /**
     * Parses a specified datetime string for its timezone.
     *
     * @param date - string representing the date in full W3C form.
     * @return datetime's timezone.
     *
     * @since v 1.02.00
     */
    public static String getTimeZoneString(String datetime){

        String tz = null;

        if (datetime != null && datetime.length() == FULL_DATETIME_LENGTH) {

            // datetime has the format: 2011-06-22T07:57:30-04:00
            tz = datetime.substring(datetime.length() - TIMEZONE_LENGTH, datetime.length());

            // check if it is a proper timezone
            if (!isTimeZoneString(tz)) {
                tz = null;
            }
        }

        return tz;
    }

    /**
     * Verifies if a given string specifies a timezone in numerical form.
     *
     * @param str - string representing the timezone (e.g. "-07:30").
     * @return true if in proper format.
     *
     * @since v 1.02.00
     */
    public static Boolean isTimeZoneString(String str) {

        Boolean result = false;

        if (str != null && str.length() == TIMEZONE_LENGTH && str.indexOf(':') == 3) {
            result = true;
        }

        return result;
    }

    /**
	 * Method receives a two dates (start and end date) and returns all dates in range in format yymmdd 
	 * @param dateInitRange
	 * @param dateEndRange
	 * @return string list representing all dates from range
	 */
    public static List<String> rangeOfDates(Date dateInitRange, Date dateEndRange) throws IllegalArgumentException {
    	if (dateInitRange.after(dateEndRange))
			throw new IllegalArgumentException("Start date ("
					+ dateInitRange.toString() + ") is after end date ("
					+ dateEndRange.toString() + ")");
    	
	    LocalDate start = new LocalDate(dateInitRange);
		LocalDate end 	= new LocalDate(dateEndRange);
		
		List<String> result 	= new LinkedList<String>();
		for (LocalDate date = start; date.isBefore(end) || date.isEqual(end); date = date.plusDays(1)){
		    String auxStr = date.getYearOfCentury() + normalizeForTwoDigits(date.getMonthOfYear()) + normalizeForTwoDigits(date.getDayOfMonth());
		    result.add(auxStr);
		}
		return result;
    }

    
	/**
	 * 
	 * @param dayOrMonth
	 * @return the String for two digits to any integer value less 10
	 */
	public static String normalizeForTwoDigits(int dayOrMonth){
		//TODO : Move this method for DateTimeUtils
		if (dayOrMonth < 10){
			return "0" + dayOrMonth;
		}
		
		return String.valueOf(dayOrMonth);
	}
    
    //</editor-fold>

}
