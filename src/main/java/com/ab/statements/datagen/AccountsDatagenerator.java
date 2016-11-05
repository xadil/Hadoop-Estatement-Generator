/**
 * 
 */
package com.ab.statements.datagen;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @author xadil
 *
 */
public class AccountsDatagenerator {
	private String[] accounts = {
			"1000000001",
			"1000000002",
			"1000000003",
			"1000000004",
			"1000000005",
			"1000000006",
			"1000000007",
			"1000000008",
			"1000000009",
			"1000000010",
			"1000000011",
			"1000000012",
			"1000000013",
			"1000000014",
			"1000000015",
			"1000000016",
			"1000000017",
			"1000000018",
			"1000000019",
			"1000000020",
			"1000000021",
			"1000000022",
			"1000000023",
			"1000000024",
			"1000000025",
			"1000000026",
			"1000000027",
			"1000000028",
			"1000000029",
			"1000000030",
			"1000000031",
			"1000000032",
			"1000000033",
			"1000000034",
			"1000000035",
			"1000000036",
			"1000000037",
			"1000000038",
			"1000000039",
			"1000000040",
			"1000000041",
			"1000000042",
			"1000000043",
			"1000000044",
			"1000000045",
			"1000000046",
			"1000000047",
			"1000000048",
			"1000000049",
			"1000000050",
			"1000000051",
			"1000000052",
			"1000000053",
			"1000000054",
			"1000000055",
			"1000000056",
			"1000000057",
			"1000000058",
			"1000000059",
			"1000000060",
			"1000000061",
			"1000000062",
			"1000000063",
			"1000000064",
			"1000000065",
			"1000000066",
			"1000000067",
			"1000000068",
			"1000000069",
			"1000000070",
			"1000000071",
			"1000000072",
			"1000000073",
			"1000000074",
			"1000000075",
			"1000000076",
			"1000000077",
			"1000000078",
			"1000000079",
			"1000000080",
			"1000000081",
			"1000000082",
			"1000000083",
			"1000000084",
			"1000000085",
			"1000000086",
			"1000000087",
			"1000000088",
			"1000000089",
			"1000000090",
			"1000000091",
			"1000000092",
			"1000000093",
			"1000000094",
			"1000000095",
			"1000000096",
			"1000000097",
			"1000000098",
			"1000000099",
			"1000000100"
	};
	
	
	String []  debitMerchants = {
		"Dewa Bill payment",
		"Etisalat bill payment",
		"DU bill payment",
		"Salik Recharge",
		"Kidszone DCC",
		"Splash DCC",
		"Zara MCC",
		"Galleries MCC",
		"Samsung DCC",
		"Jackys MOE",
		"GITEX Shopper",
		"Cheque drawn",
		"Food court",
		"west zone SM",
		"Medina SM",
		"Al Adil SM",
		"Fly Dubai",
		"Emirates petrol pump",
		"Emirates zoo",
		"dubai taxi"
	};
	
	int []  debitMaxLimits = {
		2000,
		1000,
		2000,
		200,
		500,
		5000,
		10000,
		1000,
		3000,
		3000,
		5000,
		5000,
		300,
		2000,
		3000,
		3000,
		10000,
		300,
		300,
		500
	};
	
	String []  creditMerchants = {
		"Salary Credit",
		"Cheque Cleared",
		"Reversal"
	};
	
	int []  creditMaxLimits = {
		30000,
		10000,
		2000
	};
	
	/**
	 * 
	 */
	public AccountsDatagenerator() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) {
		new AccountsDatagenerator().generateData();
	}

	private void generateData() {
		// TODO Auto-generated method stub
		Random rand = new Random();
		SimpleDateFormat formtter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		
		for(int j=0;j<10000;){
			Random rand1 = new Random();
			int i = Math.abs(rand.nextInt()*71*rand1.nextInt());
			StringBuilder buff = new StringBuilder();
			buff.append(accounts[i%100]).append("|");
			buff.append(accounts[i%100].substring(3)).append("|");
			buff.append("DR").append("|");
			buff.append(i%debitMaxLimits[i%20]+"."+i%99).append("|");
			buff.append("MT000000"+i%20).append("|");
			buff.append(debitMerchants[Math.abs((i+rand1.nextInt())%20)]).append("|");
			
			
			String day = (""+i%28);
			
			if(day.length()<2){
				day = "0"+day;
			}
			
			/*String month = (""+(i%12 == 0? 1 : i%12));
			
			if(month.length()<2){
				month = "0"+month;
			}*/
			
			String hour = (""+(i%24));
			
			if(hour.length()<2){
				hour = "0"+hour;
			}
			
			String mins = (""+i%60);
			
			if(mins.length()<2){
				mins = "0"+mins;
			}
			
			String secs = (""+i%60);
			
			if(secs.length()<2){
				secs = "0"+secs;
			}
			
			try {
				formtter.parse(day+"-10-2016 "+hour+":"+mins+":"+secs);
			} catch (ParseException e) {
				continue;
			}
			
			buff.append(day+"-10-2016 "+hour+":"+mins+":"+secs).append("|");
			buff.append("REF"+i%999999999);
			
			System.out.println(buff.toString());
			j++;
		}
		
		
		for(int k=0;k<3;k++){
			for(int j=0;j<accounts.length;){
				Random rand1 = new Random();
				int i = Math.abs(rand.nextInt()*71*rand1.nextInt());
				StringBuilder buff = new StringBuilder();
				buff.append(accounts[j]).append("|");
				buff.append(accounts[j].substring(3)).append("|");
				buff.append("CR").append("|");
				buff.append(i%creditMaxLimits[k]+"."+j%99).append("|");
				buff.append("NA").append("|");
				buff.append(creditMerchants[k]).append("|");
				
				
				String day = (""+i%28);
				
				if(day.length()<2){
					day = "0"+day;
				}
				
				/*String month = (""+(i%12 == 0? 1 : i%12));
				
				if(month.length()<2){
					month = "0"+month;
				}*/
				
				String hour = (""+(i%24));
				
				if(hour.length()<2){
					hour = "0"+hour;
				}
				
				String mins = (""+i%60);
				
				if(mins.length()<2){
					mins = "0"+mins;
				}
				
				String secs = (""+i%60);
				
				if(secs.length()<2){
					secs = "0"+secs;
				}
				
				try {
					formtter.parse(day+"-10-2016 "+hour+":"+mins+":"+secs);
				} catch (ParseException e) {
					continue;
				}
				
				buff.append(day+"-10-2016 "+hour+":"+mins+":"+secs).append("|");
				buff.append("REF"+i%999999999);
				
				System.out.println(buff.toString());
				j++;
			}
		}
	}

}
