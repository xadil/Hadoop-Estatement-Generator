/**
 * 
 */
package com.ab.statement.ordering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author xadil
 * @param <T>
 *
 */
public class StatementsKeyType implements WritableComparable {
	
	public StatementsKeyType(){
		
	}
	
	public StatementsKeyType(String accountId, String tranTime) {
		super();
		this.accountId = accountId;
		this.tranTime = tranTime;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return accountId;
	}

	private String accountId;
	private String tranTime;
	/**
	 * @return the accountId
	 */
	public String getAccountId() {
		return accountId;
	}

	/**
	 * @param accountId the accountId to set
	 */
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	
	
	/**
	 * @return the tranTime
	 */
	public String getTranTime() {
		return tranTime;
	}

	/**
	 * @param tranTime the tranTime to set
	 */
	public void setTranTime(String tranTime) {
		this.tranTime = tranTime;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput data) throws IOException {
		// TODO Auto-generated method stub
		accountId = WritableUtils.readString(data);
		tranTime = WritableUtils.readString(data);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, accountId);
		WritableUtils.writeString(out, tranTime);
	}

	public int compareTo(Object arg0) {
		StatementsKeyType passed = (StatementsKeyType)arg0;
		int cmp = accountId.compareTo(passed.getAccountId());
		
		if(cmp != 0){
			return cmp;
		}
		return tranTime.compareTo(passed.getTranTime());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg0) {
		StatementsKeyType passed = (StatementsKeyType)arg0;
		return accountId.equals(passed.getAccountId());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return accountId.hashCode();
	}
	
	
	
	

//	/* (non-Javadoc)
//	 * @see java.lang.Comparable#compareTo(java.lang.Object)
//	 */
//	public <T> int compareTo(T field) {
//		int cmp = accountId.compareTo(field.toString());
//		return 0;
//	}


}
