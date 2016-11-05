/**
 * 
 */
package com.ab.statement.ordering;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author xadil
 *
 */
public class StatementsGroupComparator extends WritableComparator {
	
	public StatementsGroupComparator(){
        super(StatementsKeyType.class,true);
    }
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		StatementsKeyType key1 = (StatementsKeyType)w1;
		StatementsKeyType key2 = (StatementsKeyType)w2;
		
		return key1.getAccountId().compareTo(key2.getAccountId());
	}

//	/* (non-Javadoc)
//	 * @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
//	 */
//	@Override
//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//		// TODO Auto-generated method stub
//		System.out.println(new String(b1));
//		System.out.println(new String(b2));
//		return 0;
//	}
	
	

}
