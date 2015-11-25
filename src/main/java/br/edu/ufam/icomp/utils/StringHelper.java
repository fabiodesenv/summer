package br.edu.ufam.icomp.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class StringHelper {
	
	private static Logger logger = Logger.getLogger(StringHelper.class);

    public static final String REG_EXP_EMAIL  =
            "^[a-zA-Z0-9_.-]{2,}@([A-Za-z0-9_-]{2,}.)+[A-Za-z]{2,4}$";

    /**
     * Verifies if the informed text is probably an e-mail.
     *
     * @param text Text that should be validated.
     *
     * @return true if the informed text is an e-mail.
     *
     */
    public static boolean isEmail(String text) {

        boolean result = false;

        if (text != null) {
            result = text.matches(REG_EXP_EMAIL);
        }

        return result;
    }

    /**
     * Verifies if the informed text is a long.
     *
     * @param text Text that should be validated.
     *
     * @return true if the informed text is a long.
     *
     */
    public static boolean isLong(String text) {

        boolean result = false;

        try {

            Long.parseLong(text);
            result = true;
        } catch (Exception ex) {
            // Ignore any exception, it means it is not a long.
        }

        return result;
    }

    /**
     * Formats a given number into a two-digit string.
     *
     * @param i - number to format
     * @return number in string form with two digits
     *
     */
    public static String formatToTwoDigits(int i) {

        if (i > 99 || i < 0) {
            throw new IllegalArgumentException(i + "is not a valid value to format.");
        }
        
        return (i < 10 ? "0" + i : "" + i);
    }
    
    /**
     * Verifies if the informed text is all numeric (all digits)
     *
     * @param text Text that should be validated.
     * @return true if the informed text is all numeric
     *
     */
    public static boolean isNumeric(String str)
    {
        for (char c : str.toCharArray())
        {
            if (!Character.isDigit(c)) return false;
        }
        return true;
    }

    /**
     * Verifies if the informed text is null or an empty string.
     *
     * @param text Text that should be validated.
     * @return true if the informed text is null or an empty string.
     *
     */
    public static boolean isNullOrEmpty(String text) {
        
        boolean ret = false;

        ret = (text == null || text.trim().equals("") ||
                text.trim().equalsIgnoreCase("null"));

        return ret;
    }

    /**
     * Extracts user name from email.
     *
     * @param email The email to extract the user name.
     * @return user name part of address, null if not an e-mail address.
     * 
     */
    public static String extractUserNameFromEmail(String email) {

        String user = null;

        try {
            user = email.substring(0, email.indexOf('@'));
        } catch(Exception e) {
            // Suppressing on purpose. Any exception in substring or indexOf
            // means the user part of the string is null.
        }

        return user;
    }

    /**
     * Replicate the character informed.
     *
     * @param text character to replicate
     * @param qty number of times to replicate the character
     * @return characters replicated
     *
     */
    public static String replicateText(String text, int qty) {

        if (qty < 2) {
            throw new IllegalArgumentException("qty is invalid");
        }

        if (text == null || text.equals("")) {
            throw new IllegalArgumentException("text is invalid");
        }

        StringBuilder result = new StringBuilder(qty);

        for (int i = 0; i < qty; i++) {
            result.append(text);
        }

        return result.toString();
    }

    public static String extractFileNameFromFileAlreadyExistsException(String errorMessage) {
    	// This function extracts the file name from a error message of org.apache.hadoop.mapred.FileAlreadyExistsException
    	// Error message template
    	//org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory /tmp/temporary/140215-1200 already exists
    	// TODO temporary workaround, find another better solution than parse a string that can change as APIs are always changing
    	
    	String result = null; 
    	
        if (isNullOrEmpty(errorMessage)) {
            throw new IllegalArgumentException("Error message cannot be null or empty.");
        }

        try {
        	int start = errorMessage.indexOf("/");
        	int end = errorMessage.indexOf("already exists")-1;
        	result = errorMessage.substring(start,end);
        } catch (Exception e) {
        	logger.error(e.getMessage());
        }

        return result;

    }
    
	public static Long generateMd5(String query) {
		Long hashMd5;
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		BigInteger hash = new BigInteger(1, md.digest(query.getBytes()));
		hashMd5 = hash.longValue();
		return new Long(hashMd5);
	}

}
