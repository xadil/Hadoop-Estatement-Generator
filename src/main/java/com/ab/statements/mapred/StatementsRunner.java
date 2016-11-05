/**
 * 
 */
package com.ab.statements.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ab.statement.ordering.StatementsGroupComparator;
import com.ab.statement.ordering.StatementsKeyType;
import com.ab.statement.ordering.StatementsPartioner;

/**
 * @author xadil
 * 
 */
public class StatementsRunner extends Configured implements Tool {

	/**
	 * 
	 */
	public StatementsRunner() {
		// TODO Auto-generated constructor stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		if (args.length != 3) {
			System.err
					.println("Usage: StatementsRunner <input path> <input path> <outputpath>");
			System.exit(-1);
		}
		conf = new Configuration();
	    Job job = Job.getInstance(conf, "Statement generator");
		job.setJarByClass(StatementsRunner.class);
		job.setJobName("Statement Generator");
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,StatementsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,AccountInformationMapper.class);
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		StatementPDFOutputFormat.setOutputPath(job, new Path(args[2]));
		
//		job.setMapperClass(StatementsMapper.class);
		job.setReducerClass(StatementsReducer.class);
		job.setMapOutputKeyClass(StatementsKeyType.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(StatementPDFOutputFormat.class);
		job.setGroupingComparatorClass(StatementsGroupComparator.class);
		job.setPartitionerClass(StatementsPartioner.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new StatementsRunner(), args);
		System.exit(exitCode);

	}

}
