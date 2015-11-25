package br.edu.ufam.icomp.utils;

import java.io.IOException;



/**
 * Copyright © 2013 Neemu. All rights reserved.<br><br>
 *
 * Methods and declarations for IHdfsDAO.
 *
 * @author faalmeid.
 *
 */
public interface IHdfsDAO {

//    /**
//     * Opens a stream connection for reading HDFS
//     *
//     * @throws IOException
//     *
//     * @since 1.00.00
//     */
//    void open() throws IOException;

    /**
     * Close a stream connection
     *
     * @since 1.01.00
     */
    void close();

    /**
     * Appends a line to HDFS file
     *
     * @param pathTo Path where file is located
     * @param content Content to append
     *
     * @throws IOException
     */
    void appendLine(String pathTo, String content) throws IOException;

    /**
     * Returns the last job result output folder
     *
     * @param outputPath Path of the last job result output folder
     *
     * @return
     * @throws HDFSException
     */
    String getLastJobResultOutputFolder(String outputPath) throws HDFSException;

    /**
     * Reads a file from HDFS
     *
     * @param path HDFS path
     * @return Content of file
     *
     * @throws HDFSException
     */
    String readFile(String path) throws HDFSException;

    /**
     * Returns the output file given an output path
     *
     * @param outputPath where the file is located
     *
     * @return the output file given an output path
     *
     * @throws HDFSException
     */
    String getOutputFile(String outputPath) throws HDFSException;

    /**
     * Reads the data file used for tests
     *
     * @param path The path of the file
     *
     * @return the data file used for tests
     */
    String readTestDataFile(String path);

    /**
     * Copy files from local to HDFS
     *
     * Actually this method creates a new file on HDFS, reads the content of the source file
     * then appends the content to the new file created.
     *
     * @param filePath source path
     * @param pathTo destination path
     *
     * @throws IOException
     */
    void copyFileToHDFS(String filePath, String pathTo) throws IOException;

}
