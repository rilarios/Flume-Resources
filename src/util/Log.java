package co.bancolombia.flume.util;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Clase que permite realizar escritura a de logs a la salida estándar y a la salida de error de los sistemas.
 * Facilita la creación de timestamps y deja un formato estándar para los Logs.<br>
 * El formato es el siguiente:<br>
 * <b>Fecha Hora Tipo [Método] Mensaje<br><br> Donde:</b>
 *  <li><b>Fecha Hora</b>:  Fecha Hora en formato :  yyyyMMdd HH:mm:ss</li>
 *  <li><b>Tipo</b>:  El tipo de mensaje, puede ser ERROR o INFO</li>
 *  <li><b>Método</b>:  Método que genera el mensaje, depende del desarrollo, puede ser Start, Stop u otro.</li>
 *  <li><b>Mensaje</b>:  mensaje enviado y que se debe procesar.</li>

 * @author rlarios
 *
 */
public class Log {
	
	private static SimpleDateFormat fmtFecha = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
	
	
	/**
	 * Genera un mensaje de Información en la salida estándar (System.out) 
	 * @param method Identificador del método que hace el llamado al Log
	 * @param mensaje Texto a escribir en la salida estándar
	 */
	public static void logInfo(String method, String mensaje){
		printLog(method,System.out,mensaje);
	}
	
	
	/**
	 * Genera un mensaje de Error en la salida de error (System.err) 
	 * @param method Identificador del método que hace el llamado al Log
	 * @param mensaje Texto a escribir en la salida de error
	 */
	public static void logError(String method, String mensaje){
		printLog(method,System.err,mensaje);
	}
	
	

	/**
	 * @param method Identificador del método que hace el llamado al Log
	 * @param tipo Tipo de PrintStream.  Debe ser System.err o System.out
	 * @param mensaje Texto a escribir en la salida de error
	 */
	private static void printLog(String method, PrintStream tipo, String mensaje){
		String type = "";
		if(tipo == System.err){
			type = "ERROR";
		}else {
			type = "INFO";
		}
			
		tipo.println(getTimeStamp() + " " + type + " [" + method + "] " + mensaje);
	}

	
	/**
	 * Genera un String con la fecha y Hora actuales
	 * @return Fecha y Hora con el formato yyyyMMdd HH:mm:ss
	 */
	private static String getTimeStamp() {
		Date now = Calendar.getInstance().getTime();
		return fmtFecha.format(now);

	}
	

}
