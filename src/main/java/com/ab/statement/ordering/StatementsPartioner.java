/**
 * 
 */
package com.ab.statement.ordering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author xadil
 *
 */
public class StatementsPartioner extends Partitioner<StatementsKeyType, Text> {

	@Override
	public int getPartition(StatementsKeyType key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return Math.abs(key.getAccountId().hashCode()*127)%numPartitions;
	}

}
