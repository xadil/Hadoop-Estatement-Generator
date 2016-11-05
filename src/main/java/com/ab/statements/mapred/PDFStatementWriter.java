/**
 * 
 */
package com.ab.statements.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;

import be.quodlibet.boxable.BaseTable;
import be.quodlibet.boxable.datatable.DataTable;

/**
 * @author xadil
 *
 */
public class PDFStatementWriter<K, V> extends RecordWriter<K,V> {
	private Path file;
	private Configuration conf;
	/**
	 * 
	 */
	public PDFStatementWriter(Path file,Configuration conf) {
		this.file = file;
		this.conf = conf;
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		generatePDF(key,value);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}
	
	/*private void generatePDF(K key, V value) throws IOException{
	    Path fullPath = new Path(file, key+".pdf");
	    FileSystem fs = file.getFileSystem(conf);
	    FSDataOutputStream fileOut = fs.create(fullPath, false);
		
		PDDocument document = new PDDocument();
		PDPage page = new PDPage();
		document.addPage( page );
		
		float fontSize = 12.0f;
		PDFont font = PDType1Font.HELVETICA_BOLD;
		
		PDPageContentStream contentStream = new PDPageContentStream(document, page);
		contentStream.setFont( font, fontSize );
		contentStream.beginText();
		
		int h = 700;
		contentStream.newLineAtOffset(10,h);
		contentStream.showText("Header ");
		contentStream.newLineAtOffset(0,-fontSize-2);
		contentStream.showText( "------------------------------------------------------------" );
		
		System.out.println("key -------------> "+key);
		
		for(Text row : (Iterable<Text>)value){
			contentStream.newLineAtOffset(0,-fontSize-2);
			contentStream.showText( row.toString() );
		}
		
		contentStream.newLineAtOffset(0,-fontSize-2);
		contentStream.showText( "------------------------------------------------------------" );
		contentStream.newLineAtOffset(0,-fontSize-2);
		contentStream.showText( "Footer " );
		
		contentStream.endText();
		contentStream.close();

		
		// Save the results and ensure that the document is properly closed:
		document.save( fileOut);
		document.close();
	}*/
	
	private void generatePDF(K key, V value) throws IOException{
	    Path fullPath = new Path(file, key+".pdf");
	    FileSystem fs = file.getFileSystem(conf);
	    FSDataOutputStream fileOut = fs.create(fullPath, false);
		
		PDDocument document = new PDDocument();
		PDPage page = new PDPage();
		page.setMediaBox(new PDRectangle(PDRectangle.A4.getHeight(), PDRectangle.A4.getWidth()));
		document.addPage( page );
		
		float margin = 30;
        float tableWidth = page.getMediaBox().getWidth() - (2 * margin);
        float yStartNewPage = page.getMediaBox().getHeight() - (2 * margin);
        float yStart = yStartNewPage;
        float bottomMargin = 30;
        
        ArrayList<String[]> dataSplit = (ArrayList<String[]>)value;
        

        List<List> tableData = new ArrayList();
        String[] headerFeildValues = dataSplit.get(0);
        tableData.add(new ArrayList<String>(
                Arrays.asList("Account id ",headerFeildValues[0])));
        tableData.add(new ArrayList<String>(
                Arrays.asList("Name ",headerFeildValues[1])));
        tableData.add(new ArrayList<String>(
                Arrays.asList("Email ",headerFeildValues[2])));
        tableData.add(new ArrayList<String>(
                Arrays.asList("Mobile No ",headerFeildValues[3])));
        tableData.add(new ArrayList<String>(
                Arrays.asList("Address ",headerFeildValues[4])));
        
        headerFeildValues = dataSplit.get(1);
        tableData.add(new ArrayList<String>(
                Arrays.asList("Total Credits ",headerFeildValues[0])));
        tableData.add(new ArrayList<String>(
                Arrays.asList("Total Dedits ",headerFeildValues[1])));
        
        BaseTable dataTable = new BaseTable(yStart, 
        		yStartNewPage, bottomMargin, tableWidth, margin, document, page, true,
                                            true);
        DataTable t = new DataTable(dataTable, page);
        t.addListToTable(tableData, DataTable.NOHEADER);
        dataTable.draw();
        
        tableData = new ArrayList(); 
        //header for records
        tableData.add(new ArrayList<String>(
                Arrays.asList("Transaction Date","Reference No","Merchant Name/Source","Merchant Id","Activity Type","Amount")));
        
        //remove the first & second row
        dataSplit.remove(0);
        dataSplit.remove(0);
        //actual transaction data
        for(String[] data : dataSplit){
	        tableData.add(new ArrayList<String>(
	                Arrays.asList(data[6],data[7],data[5],data[4],data[2],data[3])));
        }
        
        BaseTable dataTableBody = new BaseTable(yStart-150, 
        		yStartNewPage, bottomMargin, tableWidth, margin, document, page, true,
                                            true);
        DataTable tbody = new DataTable(dataTableBody, page);
        tbody.addListToTable(tableData, DataTable.HASHEADER);
        dataTableBody.draw();
		
		// Save the results and ensure that the document is properly closed:
		document.save( fileOut);
		document.close();
	}

}
