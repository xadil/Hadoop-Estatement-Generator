/**
 * 
 */
package com.ab.statements.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author xadil
 * 
 */
public class PDFWriter<K, V> extends RecordWriter<K,V> {

	private DataOutputStream out;

	public PDFWriter(DataOutputStream out,String keyValueSeparator) throws IOException {
		this.out = out;
		out.writeBytes("Header\n-----------------\n");
	}

	public void write(K key, V value) throws IOException {
		for(Text row : (Iterable<Text>)value){
			out.writeBytes(row.toString());
			out.writeBytes("\n-----------------\n");
		}
		//out.writeBytes("\nBody\n-----------------\n");
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		try {
			out.writeBytes("\nFooter\n-----------------\n");
		} finally {
			// even if writeBytes() fails, make sure we close the stream
			out.close();
		}
	}

}
