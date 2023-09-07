package co.bancolombia.flume.sinks.sql;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Clase que almacena los queries utilizados por el Sink para insertar en las distintas bases de datos.
 * @author rlarios
 *
 */
public class Queries {

	private static SimpleDateFormat fmtFecha = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat fmtHora = new SimpleDateFormat("HHmmss");

	/**
	 * Redondea un valor a un numero de posiciones decimales
	 * 
	 * @param value el valor a ser redondeado
	 * @param places el numero de posiciones decimales
	 * @return el numero redondeado
	 */
	private static double round(double value, int places) {
		if (places < 0) throw new IllegalArgumentException();

		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}

	/**
	 * Retorna un valor convertido de un string a un double
	 * @param fld String con el valor a convertir
	 * @return el valor convertido en doble
	 */
	private static double toDouble(String fld){
		return round(Double.parseDouble(fld),2);
	}

	/**
	 * Retorna un valor convertido de un string a un long
	 * @param fld String con el valor a convertir
	 * @return el valor convertido en long
	 */
	private static long toLong(String fld){
		return Long.parseLong(fld);
	}


	/**
	 * Retorna la fecha actual en formato yyyyMMdd
	 * @return la fecha actual
	 */
	private static long getFecha() {
		Date now = Calendar.getInstance().getTime();
		return toLong(fmtFecha.format(now));
	}

	/**
	 * Retorna la hora actual en formato HHmmss
	 * @return la hora actual
	 */
	private static long getHora() {
		Date now = Calendar.getInstance().getTime();
		return toLong(fmtHora.format(now));
	}

	/**
	 * Genera la Hora del servidor.  Es una hora para pasar al query para que sea calculada en el momento de la inserción.
	 * @return El String con el formato de creación de Hora.  En SQL Server es HHMMSS.
	 */
	private static String getHoraSQL(){
		return " Convert (int , replace(Convert (varchar(8),GetDate(), 108),':','') ) ";
	}


	/**
	 * Genera la Hora del servidor.  Es una hora para pasar al query para que sea calculada en el momento de la inserción.
	 * @return El String con el formato de creación de Hora.  En SQL Server es HH:MM:SS.
	 */
	private static String getHoraSQLPuntos(){
		return " Convert (varchar(8),GetDate(), 108)  ";
	}



	/*
	 * !!!! BEHOLD !!! MAGIC NUMBERS !!!!!
	 * 
	 * ___________________6666666___________________ 
	 * ____________66666__________66666_____________ 
	 * _________6666___________________666__________ 
	 * _______666__6____________________6_666_______ 
	 * _____666_____66_______________666____66______ 
	 * ____66_______66666_________66666______666____ 
	 * ___66_________6___66_____66___66_______666___ 
	 * __66__________66____6666_____66_________666__ 
	 * _666___________66__666_66___66___________66__ 
	 * _66____________6666_______6666___________666_ 
	 * _66___________6666_________6666__________666_ 
	 * _66________666_________________666_______666_ 
	 * _66_____666______66_______66______666____666_ 
	 * _666__666666666666666666666666666666666__66__ 
	 * __66_______________6____66______________666__ 
	 * ___66______________66___66_____________666___ 
	 * ____66______________6__66_____________666____ 
	 * _______666___________666___________666_______ 
	 * _________6666_________6_________666__________ 
	 * ____________66666_____6____66666_____________ 
	 * ___________________6666666________________
	 * 
	 * 
	 * Todos los queries reciben un Array de campos (los cuales son recibidos por flume desde spark).  La definicion de cada campo es como sigue:
	 * fecha,hora,documento,tipo_doc,vlr,ctadeb,convenio,nomconven,codtrn,canal,celular,dsctrn,horarcv,times_bd2,colfec,horaFlume,AlertaNombre,VlrAcum,CntAcum
	 * 0      1   2         3        4    5        6         7      8     9       10     11      12        13      14         15         16       17     18 
	 */



	/**
	 * Genera el query de insercion de datos con los campos del query.  Este query es para insertar un bloqueo de clave en la base de datos de Minería
	 * @param campos El array de campos enviado desde spark
	 * @param codop Codigo de operación (es un campo adicional)
	 * @return El query a ejecutar
	 * @throws Exception En el caso de que haya un error generando el query
	 */
	public static String bloqueoDirecto(String[] campos, int codop) throws Exception{
		//fecha,hora,documento,tipo_doc,vlr,ctadeb,convenio,nomconven,codtrn,canal,celular,dsctrn,horarcv,times_bd2,colfec,horaFlume,AlertaNombre,VlrAcum,CntAcum
		//0      1   2         3        4    5        6         7      8     9       10     11      12        13      14         15         16       17     18 

		try{
			//return "INSERT INTO REGLAS_FILES.BLOQUEO_DIRECTO (NUMDOC, HORA, FECHA, TIPDOC, CODOP, TARJETA, REGLA) "
			return "INSERT INTO TMP_DATA.BLOQUEO_DIRECTO (NUMDOC, HORA, FECHA, TIPDOC, CODOP, TARJETA, REGLA) "
				+ "VALUES (" + (campos[2]) +  ","  + getHoraSQL()  +  "," + getFecha() +  "," 
				+ (campos[3]) + "," + codop + "," +  0 + ",'" + campos[16] + "')";
		}catch(Exception e){
			throw new Exception("Error Insertando Valores [bloqueoDirecto][" + e.getClass().getName() + "]=" + e.getMessage() );
		}
	} 


	/**
	 * Genera el query de insercion de datos con los campos del query.  Este query es para insertar un bloqueo de clave en la base de datos de MEDELLIN-iSeries
	 * @param campos El array de campos enviado desde spark
	 * @param codop Codigo de operación (es un campo adicional)
	 * @return El query a ejecutar
	 * @throws Exception En el caso de que haya un error generando el query
	 */
	public static String bloqueoISeries(String[] campos, int codop) throws Exception{
		try{
			return "INSERT INTO MATLIBRAMD.MATFFNVMOD (CODOP, NUMDOC, TIPDOC, TARJETA, FECHA, HORA, LGREGLA)  "
					+ "VALUES (" + codop + "," + (campos[2]) +  "," +  (campos[3]) +  ","
					+ 0 + "," + getFecha() +  "," + getHora() + ",'RECARGAS')";
		}catch(Exception e){
			throw new Exception("Error Insertando Valores [bloqueoISeries][" + e.getClass().getName() + "]=" + e.getMessage() );
		}
	}

	/**
	 * Genera el query de insercion de datos con los campos del query.  Este query es para insertar una alerta en la tabla de Recargas-ReglasR
	 * @param campos El array de campos enviado desde spark
	 * @return El query a ejecutar
	 * @throws Exception En el caso de que haya un error generando el query
	 */
	public static String r_RECARGASTIGO(String[] campos) throws Exception{
		try{
			String llave = getLLave(campos);
			//return "INSERT INTO REGLAS.R_RECARGASTIGO (LLAVE, SORTINDEX, BLOQUEAR, FECHA, TIPODOC, DOCUMENTO, CUENTA, TOTALDIA, #TRXS, HORA, VALOR, CANAL, CODTRN, DSCTRN, CONVENIO, NOMBRE_CONVENIO, CELULAR)  "
			return "INSERT INTO TMP_DATA.R_RECARGASTIGO (LLAVE, SORTINDEX, BLOQUEAR, FECHA, TIPODOC, DOCUMENTO, CUENTA, TOTALDIA, #TRXS, HORA, VALOR, CANAL, CODTRN, DSCTRN, CONVENIO, NOMBRE_CONVENIO, CELULAR)  "
			+ "VALUES ('" + llave + "'," + 0 + "," + "'SI'" + "," + getFecha() + "," + (campos[3]) + "," + (campos[2]) + "," 
			+  (campos[5]) + "," + toDouble(campos[17]) + "," + (campos[18]) + "," + getHoraSQLPuntos() + "," + toDouble(campos[4]) 
			+ ",'" + campos[9] + "'," + campos[8]  + ",'" + campos[11] + "'," + campos[6] + ",'" + campos[7] + "'," + campos[10]
					+ ")";
		}catch(Exception e){
			throw new Exception("Error Insertando Valores [r_RECARGASTIGO][" + e.getClass().getName() + "]=" + e.getMessage() );
		}
	}


	/**
	 * Genera el query de insercion de datos con los campos del query.  Este query es para insertar una alerta en la tabla de Recargas-ReglasG
	 * @param campos El array de campos enviado desde spark
	 * @return El query a ejecutar
	 * @throws Exception En el caso de que haya un error generando el query
	 */
	public static String g_RECARGASTIGO(String[] campos) throws Exception{
		try{
			String llave = getLLave(campos);
			//return "INSERT INTO REGLAS.G_RECARGASTIGO  (LLAVE, FECHA, ALERTA_HORA, ALERTA_USUARIO, ALERTA_GESTION, ALERTA_HORAGESTION, ALERTA_OBSERVACIONES)  "
			return "INSERT INTO TMP_DATA.G_RECARGASTIGO  (LLAVE, FECHA, ALERTA_HORA, ALERTA_USUARIO, ALERTA_GESTION, ALERTA_HORAGESTION, ALERTA_OBSERVACIONES)  "
			+ "VALUES ('" + llave + "'," + getFecha() + ", GetDate() ,'LAGESAUTO','Bloqueo', GetDate() , 'No Acostumbra Recarga'"
			+ ")";
		}catch(Exception e){
			throw new Exception("Error Insertando Valores [g_RECARGASTIGO][" + e.getClass().getName() + "]=" + e.getMessage() );
		}
	}


	/**
	 * Genera una llave usada en las bases de datos de minería
	 * @param campos El array de campos enviado desde spark
	 * @return la clave con formato RCT-CuentaDebito-Fecha-Hora
	 */
	private static String getLLave(String[] campos) {
		return "RCT" + campos[5] + getFecha() + getHora();
	}


}
