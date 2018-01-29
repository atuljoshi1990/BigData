import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

public class InFlateFile {

	public static void main(String[] args) throws IOException {
		
		FileInputStream inputFile = new FileInputStream(args[0]);
		InflaterInputStream uncompressFile = new InflaterInputStream(inputFile);
		FileOutputStream fileOut = new FileOutputStream("out.txt");
		decompress(uncompressFile, fileOut);
		BufferedReader in = new BufferedReader(new FileReader("out.txt")); 
		while (in.readLine() != null) {
		      System.out.println(in.readLine());          
		}         
		in.close();
		File tempFile = new File("out.txt");
		tempFile.delete();
	}

	private static void decompress(InputStream uncompressFile,
			FileOutputStream out) throws IOException {
		int oneByte;
		while((oneByte = uncompressFile.read())!=-1){
			out.write(oneByte);
		}
		uncompressFile.close();
		out.close();
	}
}

