package br.edu.ufam.icomp.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class FileUtils {
	private static Logger logger = Logger.getLogger(FileUtils.class);

	public static void main(String[] args) {
		if ( args.length > 0 && args.length <= 2 ) {
			final File folder = new File(args[0]);
			
			String extension = null;
			if (args.length == 2)
				extension = args[1];
				

			if (folder.exists()) {				
				List<File> result = listFilesForFolder(folder, extension);
				for (File f : result)
					System.out.println(f.getName());
			}
			else
				System.out.println("Folder " + args[0] + " do not exist.");
			
		} else {
			System.out.println("Usage: <java_program> <path_folder> [extension]");
			System.out.println("Example: java -jar CSVParser /home/user/files .csv");
			System.exit(-2);
		}
	}

	// recursively read all file bellow a parent folder
	// list all csv and json files
	public static List<File> listFilesForFolder(File folder) {
		return listFilesForFolder(folder, null);
	}
	
	// recursively read all file bellow a parent folder 
	public static List<File> listFilesForFolder(File folder, String extension) {

		List<File> filelist = new ArrayList<File>();
		for (final File fileEntry : folder.listFiles()) {

			if (fileEntry.isDirectory()) {
				filelist.addAll(listFilesForFolder(fileEntry, extension));
			} else {

				if (extension != null && extension != "") {
					if ( (fileEntry.getName().endsWith(extension)) || (extension.compareToIgnoreCase("noextension") == 0))
						filelist.add(fileEntry);
				} else if ((fileEntry.getName().endsWith(Constants.CSV_EXT))
						|| (fileEntry.getName().endsWith(Constants.JSON_EXT)) )
					filelist.add(fileEntry); // filelist.add(fileEntry.getAbsoluteFile() );
			}

		}
		return filelist;
	}
	
	public static String readSmallFiles(File file) {
		FileInputStream fis;
		try {
			fis = new FileInputStream(file);
			
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			fis.close();

			return new String(data, "UTF-8");
		} catch (FileNotFoundException e) {
			logger.error("FileNotFoundException when readSmallFiles. Error: " + e.getMessage());
		} catch (IOException e) {
			logger.error("IOException when readSmallFiles. Error: " + e.getMessage());
		}
		return null;
	}
	
	public static File getCSVFromJSON(File file, List<File> filelist){
		
		for (File f : filelist) {
//			String teste = f.getName();
//			String teste2 = file.getName();
//			String teste3 = file.getName().split("_")[0];
			
			if ( f.getName().startsWith(file.getName().split("_")[0]) && f.getName().endsWith(Constants.CSV_EXT) )
				return f;
		}
		
		logger.error("Could not find a CSV file for " + file.getName());
		return null;
	}
}
