/**
 * 
 */
package com.ab.statements.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author xadil
 *
 */
public class StatementPDFOutputFormat<K,V> extends FileOutputFormat<K,V> {
	public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		Path file = getDefaultWorkFile(job, "");
		
		return new PDFStatementWriter<K,V>(file,job.getConfiguration());
	}
	
	/*@Override
	public RecordWriter<K, V> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
	    boolean isCompressed = getCompressOutput(job);
	    String keyValueSeparator= conf.get(SEPERATOR, "\t");
	    CompressionCodec codec = null;
	    String extension = "";
	    if (isCompressed) {
	      Class<? extends CompressionCodec> codecClass = 
	        getOutputCompressorClass(job, GzipCodec.class);
	      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
	      extension = codec.getDefaultExtension();
	    }
	    Path file = getDefaultWorkFile(job, extension);
	    Path fullPath = new Path(file, "result.txt");
	    FileSystem fs = file.getFileSystem(conf);
	    if (!isCompressed) {
	      FSDataOutputStream fileOut = fs.create(fullPath, false);
	      return new PDFWriter<K, V>(fileOut, keyValueSeparator);
	    } else {
	      FSDataOutputStream fileOut = fs.create(fullPath, false);
	      return new PDFWriter<K, V>(new DataOutputStream
	                                        (codec.createOutputStream(fileOut)),
	                                        keyValueSeparator);
	    }
	}*/
	
	
	
}
