package br.edu.ufam.icomp.utils;

/**
 * Copyright 2010-2011 Neemu. All rights reserved.<br><br>
 *
 * Warper Exception to be thrown on any setup or creation issues occurred while
 * trying to interact with HDFS in DAO instances.
 *
 * @author faalmeid.
 *
 * 
 * @since 
 */
public class HDFSException extends Exception {

    public HDFSException(String message) {
        super(message);
    }

    public HDFSException(String message, Throwable t) {
        super(message, t);

    }

}

