package br.edu.ufam.icomp.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import br.edu.ufam.icomp.exception.FatalIndexerException;


/**
 * Copyright © 2013 Neemu. All rights reserved.<br><br>
 *
 * Methods and declarations for HdfsDAO.java.
 *
 * @author faalmeid.
 *
 *
 * @since 1.00.00
 */
public class HDFSUtil implements IHdfsDAO {
    private static final String BREAK_LINE = ":\n";
    private static final String ERROR_LABEL = "ERROR: ";

    private static final Logger logger = Logger.getLogger(HDFSUtil.class);

    private static final int MAX_SESSIONS_IN_MEMORY = 1000;


    /*
     * These static fields together with the static initialization code are here
     * because they are only needed once, for all instances of this class.
     */
    private static final Configuration conf = new Configuration();
    private static FileSystem fs = null;

    //<editor-fold defaultstate="collapsed" desc=" Variables ">

    //private Path path;

    private FSDataInputStream fsInput;

    //</editor-fold>

    static {

        try {
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            logger.error(ERROR_LABEL + e.getMessage() + BREAK_LINE, e);
        }
    }
    
    public static void main(String[] args) {
    	if ( args.length == 2 ) {
    		String option = args[0];
    		String path = args[1];
    		
    		if(option.equalsIgnoreCase("setPermission")){
    			try {
					HDFSUtil hdfs = new HDFSUtil();
					hdfs.setPermission(path+"/*");
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
    		}else{
    			System.out.println("Option not permmited. Use: setPermission");
    		}
    	}else{
    		System.out.println("Usage: <java_program> <option> <path>");
			System.out.println("Example: java -jar pp-mapreduces com.neemu.commons.hdfsUtil.HDFSUtil setPermission /tmp/output");
			System.exit(-2);
    	}
    }

    //<editor-fold defaultstate="collapsed" desc=" Constructors ">

    /**
     * Default constructor. Defined here because of the other
     * parameterized constructor.
     */
    public HDFSUtil() {

    }

    /**
     *
     * Creates an instance of HdfsDAO. Allows for custom HDFS path definition.
     * Useful for test purposes.
     *
     * @param filePath
     *
     * @since v 1.00.00
     */
//    public HDFSUtil(String filePath) {
//        this.path = new Path(filePath);
//    }

    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc=" Public Methods ">

//    //@Override
//    public void open() throws IOException {
//
//        if (fs.exists(path)) {
//
//            fsInput = fs.open(path);
//            reader = new BufferedReader(new InputStreamReader(fsInput));
//        }
//
//    }

    //@Override
    public void close() {

        if (this.fsInput != null) {

            try {
                this.fsInput.close();
            } catch (Exception e) {
                logger.error("Error closing file: " + e.getMessage() + BREAK_LINE, e);
            }
        }
    }

    //@Override
    public void appendLine(String pathTo, String content) throws IOException {

        if (!StringHelper.isNullOrEmpty(content)) {

            Path pathToAppend = new Path(pathTo);

            FSDataOutputStream fsout = null;
            //DataOutputStream fsout = null;
            if (!fs.exists(pathToAppend)) {
                fsout = fs.create(pathToAppend);
            } else {
                fsout = fs.append(pathToAppend);
            }

            //PrintWriter writer = new PrintWriter(new OutputStreamWriter(fsout, "UTF-8"));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout, "UTF-8"));

            //PrintWriter writer = new PrintWriter(fsout);
            writer.append(content + "\n");
            writer.close();

            fsout.close();
        }
    }

    /*
     * Make the given file and all non-existent parents into directories.
     * Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
     * */
    public void createHDFSFolder(String pathToCreate) {

        Path hdfsFolderName = new Path(pathToCreate);

        if (!StringHelper.isNullOrEmpty(pathToCreate)) {

            try {

                if (!fs.exists(hdfsFolderName)) {
                    fs.mkdirs(hdfsFolderName);
                }
            } catch (IOException e) {
                logger.error("Unable to create folder "+pathToCreate+"Error: "+ e.getMessage());
            }
        }
    }

    public void setPermission(String filePath) {
        try {
            FsShell shell = new FsShell(conf);
            shell.run(new String[]{"-chmod","-R","777",filePath});
        } catch (Exception e) {
            logger.error("Unable to give permission to folder "+filePath+"Error: "+ e.getMessage());
        }
    }

    public void appendFromFolderToFile(String source, String target) throws IOException, HDFSException {

        FileSystem fsSource = FileSystem.get(conf);

        Path pathSource = new Path(source);

        // just append directories
        if (fsSource.getFileStatus(pathSource).isDir()) {

            Date folderSourceDate = DateTimeUtils.parseDateString(pathSource.getName(), "yyMMdd-HHmm");

            // create output folder if does not exists
            String outputFolderAnfFileName = DateTimeUtils.formatFullDateToString(folderSourceDate,"yyMMdd");
            createHDFSFolder(target+ File.separator+ outputFolderAnfFileName);
            String fileTargetName = target+ File.separator+ outputFolderAnfFileName + File.separator + outputFolderAnfFileName;

            FileStatus[] files = fsSource.listStatus(pathSource);

            // loop opening each file
            for (FileStatus file : files) {
                // files that start with _ are from Hadoop control only
                if (!file.getPath().getName().startsWith("_")) {

                    logger.info("Appending " + file.getPath().toString() + " to "+ fileTargetName);

                    InputStream is       = null;
                    StringBuilder buffer = new StringBuilder();
                    BufferedReader fileReader;

                    int count = 0;
                    try { // TODO append buffer, not string, will fail for really huge sessions
                        is = fsSource.open(file.getPath());

                        //fileReader = new BufferedReader(new InputStreamReader(is));
                        fileReader = new BufferedReader(new InputStreamReader(is,"UTF-8"));

                        String line = fileReader.readLine();
                        if (line != null) count++;

                        while (line != null) {
                            buffer.append(line);
                            line = fileReader.readLine();


                            if (line != null) {
                                count++;
                                buffer.append("\n");
                                logger.debug("Loading line " +count+" from file "+file.getPath().getName()+". Content: "+line);

                                if (count>=MAX_SESSIONS_IN_MEMORY) {
                                    appendLine(fileTargetName, buffer.toString()); // TODO could be an object array instead of a simple and limited string
                                    buffer = new StringBuilder();
                                    count = 0;
                                    logger.info("Appending " + file.getPath().toString() + " to "+ fileTargetName);
                                }
                            }
                        }
                    } catch (IOException ex) {
                        throw new HDFSException(ex.getMessage(), ex);
                    } finally {
                        if (is != null) {
                            try {
                                is.close();
                            } catch (Exception ex) {
                                logger.error(ERROR_LABEL + ex.getMessage() + BREAK_LINE + ex.getStackTrace().toString());
                            }
                        }
                    }

                    // TODO append a buffer, not a string
                    if (count>0) {
                        appendLine(fileTargetName, buffer.toString());
                        logger.info("Appending last lines from " + file.getPath().toString() + " to "+ fileTargetName);
                    }
                }
            }
        } else
            throw new IOException("Source folder is not a folder: " + source);
    }

    //@Override
    public String getLastJobResultOutputFolder(String outputPath) throws HDFSException {

        InputStream is = null;

        try {

            Path out = new Path(outputPath);

            if (fs.getFileStatus(out).isDir()) {

                FileStatus[] files = fs.listStatus(out);
                long current = 0;
                FileStatus last = null;

                for (int i = 0; i < files.length; i++) {

                    if (files[i].getModificationTime() > current) {
                        last = files[i];
                        current = files[i].getModificationTime();
                    }
                }

                if (last == null || last.getPath() == null) {
                    throw new HDFSException("Informed path did not contain output results folder.");
                }

                return last.getPath().toString();
            }

            throw new HDFSException("Informed path did not resolve to a directory.");

        } catch (IOException e) {

            throw new HDFSException(e.getMessage(), e);

        } finally {

            if (is != null) {

                try {
                    is.close();
                } catch (IOException e) {
                    logger.error(ERROR_LABEL + e.getMessage() + BREAK_LINE + e.getStackTrace().toString());
                }
            }
        }
    }

    //@Override
    public String readFile(String path) throws HDFSException {

        InputStream is = null;

        StringBuilder buffer = new StringBuilder();

        Path resultsPath = new Path(path);

        BufferedReader fileReader;

        try {

            is = fs.open(resultsPath);

            fileReader = new BufferedReader(new InputStreamReader(is));

            String line = fileReader.readLine();

            while (line != null) {

                buffer.append(line);
                line = fileReader.readLine();
                if (line != null) {
                    buffer.append("\n");
                }
            }

        } catch (IOException ex) {

            throw new HDFSException(ex.getMessage(), ex);

        } finally {

            if (is != null) {

                try {
                    is.close();
                } catch (Exception ex) {
                    logger.error(ERROR_LABEL + ex.getMessage() + BREAK_LINE + ex.getStackTrace().toString());
                }
            }
        }

        return buffer.toString();
    }

    //@Override
    public String getOutputFile(String outputPath) throws HDFSException {

        InputStream is = null;

        try {

            Path out = new Path(outputPath);

            FileStatus[] files = fs.listStatus(out);

            for (int i = 0; i < files.length; i++) {

                if (files[i].getPath().getName().startsWith("part-")) {
                    return files[i].getPath().toString();
                }
            }

            throw new HDFSException("Informed path does not contain any output file.");

        } catch (IOException e) {

            throw new HDFSException(e.getMessage(), e);

        } finally {

            if (is != null) {

                try {
                    is.close();
                } catch (IOException e) {
                    logger.error(ERROR_LABEL + e.getMessage() + BREAK_LINE + e.getStackTrace().toString());
                }
            }
        }
    }

    /*
     * faalmeid: Ok, it sounds strange to put this thing here
     */
    //@Override
    public String readTestDataFile(String path) {

        InputStream is = null;
        BufferedReader readerDataFile;

        StringBuilder buffer = new StringBuilder();

        //reads data to append
        try {

            is = this.getClass().getResourceAsStream(path);
            readerDataFile = new BufferedReader(new InputStreamReader(is));
            String line = readerDataFile.readLine();

            while (line != null) {

                buffer.append(line);
                line = readerDataFile.readLine();
            }

        } catch (Exception e) {

            logger.info("Error reading file: " + e.getMessage() + BREAK_LINE + e.getStackTrace().toString());

        } finally {

            if (is != null) {

                try {
                    is.close();
                } catch (Exception e) {
                    logger.error(ERROR_LABEL + e.getMessage() + BREAK_LINE + e.getStackTrace().toString());
                }
            }
        }

        return buffer.toString();
    }

    //@Override
    public void copyFileToHDFS(String filePath, String pathTo) throws IOException {

        org.apache.hadoop.fs.Path dstPath = new org.apache.hadoop.fs.Path(pathTo);

        if (!fs.exists(dstPath)) {

            String content = this.readTestDataFile(filePath);

            this.appendLine(pathTo, content);
        }
    }

    public void deleteHdfsFolderContent(String hdfsPath){

        try {

            Path path = new Path(hdfsPath);
            if (fs.exists(path)) {
                FileStatus[] fileStatus = listStatus(hdfsPath);
                //try to delete the path and throw exception if it fails
                for (FileStatus fstat : fileStatus) {
                    if (!fs.delete(path, true)) {
                        throw new FatalIndexerException("Could not delete directory under HDFS path: " + hdfsPath);
                    }
                }
            }

        } catch (Exception e) {
             logger.error("Error while trying to delete HDFS path: " + hdfsPath, e);
             throw new FatalIndexerException(e.getMessage());
        }
    }

    public void deleteHdfsPath(String hdfsPath){

        try {

            Path path = new Path(hdfsPath);

            if (fs.exists(path)) {

                //try to delete the path and throw exception if it fails
                if (!fs.delete(path, true)) {
                    throw new FatalIndexerException("Could not delete directory under HDFS path: " + hdfsPath);
                }
            }

        } catch (Exception e) {
             logger.error("Error while trying to delete HDFS path: " + hdfsPath, e);
             throw new FatalIndexerException(e.getMessage());
        }
    }

    public void deleteHdfsPathList(List<String> paths){
        for (String path : paths){
            deleteHdfsPath(path);
        }
    }

    /*
     * Wrapper for listFiles
     * FabioMoreira
     * */
//    public void listFiles(String hdfsPath){
//        listFiles(hdfsPath, false);
//    }


    /*
     * List the statuses of the files/directories in the given path if the path is a directory.
     * FabioMoreira
     * */
    public static FileStatus[] listStatus(String hdfsPath) throws FileNotFoundException,
            IOException {

        Path path = new Path(hdfsPath);

        FileStatus[] fileStatus = null;

        if (fs.exists(path)) {
            fileStatus = fs.listStatus(path);
            for (FileStatus fstat : fileStatus) {
                //System.out.println(fstat.getPath());
            }
        }
        return fileStatus;

    }

    public String getOldestFolder(String outputPath) throws HDFSException {

        InputStream is = null;

        try {

            Path out = new Path(outputPath);

            // just work with directories
            if (fs.getFileStatus(out).isDir()) {

                FileStatus[] files = fs.listStatus(out);
                Date oldestDate = null;
                FileStatus oldestFile = null;

                for (int i = 0; i < files.length; i++) {

                    Date current = DateTimeUtils.parseDateString(files[i].getPath().getName(),"yyMMdd-HHmm");

                    if (oldestDate == null) {
                        oldestFile = files[i];
                        oldestDate = current;
                    } else if (current.compareTo(oldestDate)<0) {
                        oldestFile = files[i];
                        current = oldestDate;
                    }
                }

                if (oldestDate == null || oldestFile.getPath() == null)
                    return null; // there is no file remaining, all have been processed
                else
                    return oldestFile.getPath().toString();
//                {
//                    throw new HDFSException("Informed path did not contain output results folder.");
//                }


            }

            throw new HDFSException("Informed path did not resolve to a directory.");

        } catch (IOException e) {

            throw new HDFSException(e.getMessage(), e);

        } finally {

            if (is != null) {

                try {
                    is.close();
                } catch (IOException e) {
                    logger.error(ERROR_LABEL + e.getMessage() + BREAK_LINE + e.getStackTrace().toString());
                }
            }
        }
    }

    public String[] getOldestFolders(String outputPath) throws HDFSException {
        return getOldestFolders(outputPath, -1);
    }

    public String[] getOldestFolders(String outputPath, int limit) throws HDFSException {
        Date dates[] = null;
        try {
            Path out = new Path(outputPath);

            // just work with directories
            if (fs.getFileStatus(out).isDir()) {

                FileStatus[] files = fs.listStatus(out);

                if (files.length == 0) {
                    return null; // there is no file remaining, all have been processed
                } else {
                    dates = new Date[files.length];

                    for (int i = 0; i < files.length; i++) {

                        Date current = null;

                        if (files[i].getPath().getName().length()=="yyMMdd-HH".length()) {  // it is missing minutes in folder name
                            current = DateTimeUtils.parseDateString(files[i].getPath().getName()+"00", "yyMMdd-HHmm");
                            dates[i] = current;
                            logger.warn("Invalid folder name, adding two zeros as minutes: " + files[i].getPath().getName()+"00");
                        }
                        else if (files[i].getPath().getName().length()=="yyMMdd-HHmm".length()) {
                            current = DateTimeUtils.parseDateString(files[i].getPath().getName(), "yyMMdd-HHmm");
                            dates[i] = current;
                        }
                        else
                            logger.error("Invalid date format: " + files[i].getPath().getName().length());
                    }


                    int totalNotNullDates = 0;
                    // count total of not null dates
                    for (int i = 0; i < dates.length; i++)
                        if (dates[i]!=null) totalNotNullDates++;

                    Date notNullDates[] =  new Date[totalNotNullDates];
                    int notNull = 0;
                    for (int i = 0; i < dates.length; i++) {
                        if (dates[i]!=null) {
                            notNullDates[notNull] = dates[i];
                            notNull++;
                        }
                    }


                    // sort in ascending order
                    Arrays.sort(notNullDates);

                    int maximum_folders_to_process = files.length - (dates.length - notNullDates.length);
                    if (limit>0 && limit<maximum_folders_to_process)
                        maximum_folders_to_process = limit;

                    String oldestFolders[] = new String[maximum_folders_to_process];
                    for (int i = 0; i < maximum_folders_to_process; i++) {
                        oldestFolders[i] = DateTimeUtils.formatFullDateToString(notNullDates[i], "yyMMdd-HHmm");
                        logger.debug("Ordered ascending folder["+i+"]: " + oldestFolders[i]);
                    }

                    return oldestFolders;

                }
            }
            throw new HDFSException("Informed path did not resolve to a directory.");
        } catch (IOException e) {
            throw new HDFSException(e.getMessage(), e);
        }
    }


/**
 * moveDirectory - Renames Path src to Path dst
 *     - Fails if src is a file and dst is a directory.
 *     - Fails if src is a directory and dst is a file.
 *     - Fails if the parent of dst does not exist or is a file.
 * faalmeid
 * */
    public void moveDirectory(String src, String dst) throws HDFSException {

        Path source  = new Path(src);
        Path destiny = new  Path(dst);

        //Boolean result = false;

        // just work with directories
        try {
            // if source directory exists
            if (fs.getFileStatus(source).isDir()) {
                // if parent's destiny directory exists
                if (fs.exists(destiny)) {
                    logger.debug("Moving from "+src+" to "+ src+destiny.getName());
                    Path newFolder = new Path(src+destiny.getName());
                    if (!fs.exists(newFolder)) {
                        if (fs.rename(source, destiny) == true) {
                            logger.debug("Folder was moved successfuly: "+ source.getName());
                        } else
                            throw new HDFSException(
                                    "Not possible to move folder from " + src+ " to destiny " + dst);
                    } else
                        throw new HDFSException("Destiny directory " + dst + " already exists. Error when moving folder.");
                } else throw new HDFSException("Destination folder does not exist: "+dst);
            } else
                throw new HDFSException("Source directory " + src+ " does not exists. Is it a folder or file?.");
        } catch (Exception e) {
            throw new HDFSException("Error when moving folder.Error: "+ e.getMessage());
        }
    }

    /**
     * Move the list of source directories to some destiny
     * @param src
     * @param dst
     * @throws HDFSException
     */
    public void moveDirectoryList(List<String> src, String dst) throws HDFSException {
        for (String source : src){
            moveDirectory(source, dst);
        }
    }

    public Boolean exists(String path) throws HDFSException {

        Path file = new Path(path);

        Boolean result = false;

        // just work with directories
        try {
            // if source directory exists

            if (fs.exists(file)) {
                result = true;
            }
        } catch (Exception e) {
            throw new HDFSException("Error when moving folder.Error: "
                    + e.getMessage());
        }

        return result;
    }

    public List<String> getPathsInFolder(Path path) throws HDFSException {
        try {
            // just work with directories
            if (fs.getFileStatus(path).isDir()) {

                FileStatus[] files = fs.listStatus(path);

                if (files.length == 0) {
                    return null; // there is no file remaining, all have been processed
                } else {
                    String folders[] = new String[files.length];

                    for (int i = 0; i < files.length; i++) {
                        folders[i] = files[i].getPath().toString();
                    }

                    return Arrays.asList(folders);
                }
            }
            throw new HDFSException("Informed path did not resolve to a directory.");
        } catch (IOException e) {
            throw new HDFSException(e.getMessage(), e);
        }
    }
    /**
     *
     * @param pathFile
     * @param srcDestiny
     */
    public void moveFilesToFolder(String pathFile, String srcDestiny){
        try {
            Path src = new Path(pathFile);
            Path dst = new Path(srcDestiny);
            if (fs.exists(src) && fs.getFileStatus(dst).isDir()){
                fs.rename(src, dst);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public FileSystem getFs() {
        return fs;
    }



    //</editor-fold>

}
