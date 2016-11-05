/**
 * 
 */
package com.ab.statements.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ab.statement.helper.StatementHelper;
import com.ab.statement.ordering.StatementsKeyType;

/**
 * @author xadil
 *
 */
public class AccountInformationMapper extends Mapper<LongWritable, Text, StatementsKeyType, Text> {

	/**
	 * 
	 */
	public AccountInformationMapper() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		String [] fields = value.toString().split("[|]");
		
		if(fields.length == 5){
			String accountNo = fields[0];
			String tranDate = StatementHelper.getVeryOldDate();
			context.write(new StatementsKeyType(accountNo,tranDate),value);
		}
	}
	
	
}
