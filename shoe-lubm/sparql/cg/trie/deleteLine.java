package cg.trie;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class deleteLine {

	public void deleteLine(String path) throws IOException{
		File file = new File(path);

			BufferedReader reader = new BufferedReader(new FileReader(file));
			
			String line = null;
			while((line = reader.readLine()) != null){
				if(line.matches("$.*")){
					
				}
				if(line.matches(".*-1.*")){
					
				}
			}
			
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
