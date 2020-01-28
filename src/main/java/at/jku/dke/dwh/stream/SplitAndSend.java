package at.jku.dke.dwh.stream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.Scanner;

public class SplitAndSend {
	public static void main(String[] args) throws IOException, InterruptedException {
		while(true) {
			Scanner sc = new Scanner(new File("./orders.txt"));
			
			File outFile =  File.createTempFile("ord", ".tmp", new File("./orders/"));
			
			PrintWriter out = null;
			try {
			  out = new PrintWriter(new OutputStreamWriter(
			      new BufferedOutputStream(new FileOutputStream(outFile)), "UTF-8"));
			  
			  while(sc.hasNext()) {
				  String line = sc.nextLine();
				  
				  Random random = new Random();
				  boolean include = random.nextFloat() < 0.00015;
				  
				  if(include) {
					  out.println(line);
				  }
			  }
			} catch (UnsupportedEncodingException e) {
			  e.printStackTrace();
			} catch (FileNotFoundException e) {
			  e.printStackTrace();
			} finally {
			  if(out != null) {
			    out.flush();
			    out.close();
			    sc.close();
			  }
			}
			
			Thread.sleep(10000);
		}
	}
}
