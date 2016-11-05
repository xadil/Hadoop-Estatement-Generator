package com.ab.statements.mapred;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ab.statement.helper.StatementHelper;
import com.ab.statement.ordering.StatementsKeyType;

/**
 * 
 */

/**
 * @author xadil
 *
 */
public class StatementsMapper extends Mapper<LongWritable, Text, StatementsKeyType, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
//		System.out.println("key = "+key+" value = "+value);
		
		String [] fields = value.toString().split("[|]");
		
		
		
		if(fields.length == 8){
			String accountNo = fields[0];
			String tranDate = StatementHelper.parseDateAndGetLong(fields[6]);
			context.write(new StatementsKeyType(accountNo,tranDate),value);
		}
		
		//super.map(key, value, context);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

}
