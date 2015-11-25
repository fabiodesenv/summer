package br.edu.ufam.icomp.mr.FileInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

class WholeFileRecordReader3 extends RecordReader<Text, BytesWritable> {
	
	//private static final int TOTAL_FILES_WITHIN_TAR = 5000;
	private static Logger logger = Logger.getLogger(WholeFileRecordReader3.class);
	private FileSplit fileSplit;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private boolean processed = false;
	private InputStream in = null;
	private TarArchiveInputStream tarArchiveInputStream = null;
	private String currentFileName = null;
	//private long counter = 0;
	private static long MAX_LIMIT = 157286400; // 150Mb
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split; // it must have the content of an entire file (since split is false)
		this.conf = context.getConfiguration();
		
		try {
			Path inputPath = fileSplit.getPath();		
			FileSystem fs = FileSystem.get(URI.create(fileSplit.getPath().toString()), conf);
			in = fs.open(inputPath);
			
			tarArchiveInputStream = new TarArchiveInputStream( in );
		} catch (Exception e) {
			logger.error("Error initialize: " + e.getMessage());
			System.out.println("Error initialize: " + e.getMessage());
		}
	}
	
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		currentFileName = "";

		ArchiveEntry tarEntry = tarArchiveInputStream.getNextEntry();

		if ( tarEntry != null ) {
			//counter++;
			currentFileName = tarEntry.getName();
			final byte[] buffer = new byte[130560];
			
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			
			int read = 0; int n = 0;
			while (-1 != (n=tarArchiveInputStream.read(buffer))) {
		          os.write(buffer, 0, n);
		          read += n;
		          if (read > MAX_LIMIT) {
		        	  logger.error("Maximum size reached for file " + currentFileName);
		        	  break;
		          }
		    }
			value.set(os.toByteArray(), 0, os.size() );
			logger.debug("read: " + read);
			
			os.close();
		
			processed = true;
			return processed;

		}
		System.out.println("No more entries");
		return false;
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(currentFileName);
		//return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,  InterruptedException {
		return value;
	}
	
	@Override
	public float getProgress() throws IOException {
		//return (counter/TOTAL_FILES_WITHIN_TAR)*100;
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		try {
			IOUtils.closeStream(in);
			tarArchiveInputStream.close();
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
	
	public String getCurrentFileName() {
		return currentFileName;
	}

}
