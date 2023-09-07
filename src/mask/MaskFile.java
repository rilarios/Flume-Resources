package co.bancolombia.flume.mask;

import co.bancolombia.common.mask.Masker;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class MaskFile {

	public static void main(String[] args) throws IOException {
		
		String path = "D:/Analitica/Seguridad_Fase2/flumeSML/base_certi.csv";
		
		BufferedReader br = getLectorEncoding(path , "UTF-8");
		
		String line = "";
		StringBuffer sb = new StringBuffer("");
		while( (line = br.readLine()) != null){
			String[] fields = line.split(",");
			String ret = "";
			for(int i = 0 ; i < fields.length ; i++){
				if(i == 2 || i == 5 || i == 10){
					ret = ret + Masker.sha512( Long.parseLong(fields[i]) ) + ",";
				}else{
					ret = ret + fields[i] + ",";
				}
			}
			sb.append(ret.substring(0, ret.length() -1 ) + "\n");
		}
		
		OutputStreamWriter wr = getEscritorUTF8(path + "_mask_full.csv");
		wr.write(sb.toString());
		wr.flush();
		wr.close();
		
		
	}
	
	public static BufferedReader getLectorEncoding(String path,String encoding) throws UnsupportedEncodingException, FileNotFoundException{
		InputStreamReader a = new InputStreamReader(
				new FileInputStream(path), encoding);
		return new BufferedReader(a);
	}
	
	public static OutputStreamWriter getEscritorUTF8(String path) throws FileNotFoundException{
		//System.out.println("geEscritor = " + path);
		return getEscritorEncoding(path,"UTF-8");

	}
	
	public static OutputStreamWriter getEscritorEncoding(String path , String encoding) throws FileNotFoundException{
		//System.out.println("geEscritor = " + path);
		return new OutputStreamWriter(new FileOutputStream(path),Charset.forName(encoding).newEncoder());

	}

}
