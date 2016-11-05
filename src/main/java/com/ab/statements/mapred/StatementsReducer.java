/**
 * 
 */
package com.ab.statements.mapred;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.ab.statement.ordering.StatementsKeyType;

/**
 * @author xadil
 *
 */
public class StatementsReducer extends
		Reducer<StatementsKeyType, Text, Text, ArrayList<String[]>> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(StatementsKeyType statementsKeyType, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		//for(Text row : values){
		ArrayList<String[]> dataSplit = new ArrayList<String[]>();
		BigDecimal totalCredit = new BigDecimal(0);
		BigDecimal totalDebit = new BigDecimal(0);
        for(Text row : (Iterable<Text>)values){
        	String[] fields = row.toString().split("[|]");
        	if("CR".equalsIgnoreCase(fields[2])){
        		totalCredit = totalCredit.add(new BigDecimal(fields[3]));
        	}else if("DR".equalsIgnoreCase(fields[2])){
        		totalDebit = totalDebit.add(new BigDecimal(fields[3]));
        	}
        	dataSplit.add(fields);
        }
        
        if(dataSplit.size() <= 1){
        	System.out.println("No statements to be generated for key : "+statementsKeyType.getAccountId());
        	return;
        }
        
        dataSplit.add(1, new String[]{totalCredit.toPlainString(),totalDebit.toPlainString()});
//		System.out.println("key = "+statementsKeyType.toString()+" values = "+values);
		context.write(new Text(statementsKeyType.getAccountId()), dataSplit);
		//}
	}

	
}
