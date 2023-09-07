package co.bancolombia.flume.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;



public class FlumeAlive {

	private static SimpleDateFormat fmtHora = new SimpleDateFormat("yyyyMMddHHmmss");


	private static String LASTDATA = "";


	private static String getHora() {
		Date now = Calendar.getInstance().getTime();
		return fmtHora.format(now);
	}


	private static String getHoraNueva(int nh) {
		System.out.println(nh);
		if(nh < 100000){
			return "0" + nh;
		}else{
			return "" + nh;
		}
	}

	public static void main(String[] args) {

		LASTDATA = "20170704140421";

		System.out.println(	LASTDATA + " - " + getHora());
		System.out.println(	"nuevo" + " - " + getHoraNueva(Integer.parseInt( getHora().substring(8) )));
		//[AUD-Process] 
		Log.logInfo("AUD-Process", "Satus : READY, Filas Totales: 45666, Ejecucion # 218154 duracion = 29 ms. (0 s), TS:20170704183858");
		

	}

}
