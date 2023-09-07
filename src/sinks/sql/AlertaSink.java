package co.bancolombia.flume.sinks.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;





import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import co.bancolombia.flume.sources.sql.ConnectionPool;
import co.bancolombia.flume.util.Log;
import co.bancolombia.flume.util.PwDecryptor;

/**
 * Clase que se encarga de insertar en bases de datos externas la los datos recibidos por le flujo de flume.
 * 
 * En este caso este sink recibe del canal un array de strings, el cual usa para generar los queries de inserción. <br>
 * El sink debe recibir los elementos para generar una configuración a las distintas bases de datos a insertar los datos.
 * 
 *  
 * 
 * @author rlarios
 *
 */
public class AlertaSink  extends AbstractSink implements Configurable {


	//Ultimos Cambios - Modificación de Loggeo
	//                - Solo bloqueo de primera clave
	//			  	  - Se añade soporte para criptografía
	//			  	  - Modificación Mensajes de Error
	//			  	  - Modificación debug
	private String VERSION = "1.4 [20171110]";

	private String urlSeg;
	private String driverSeg;
	private String usrSeg;
	private String passSeg;
	private Connection connSeg;
	private Connection connISeries;
	private String urlISeries;
	private String driverISeries;
	private String usrISeries;
	private String passISeries;
	private int numConn;
	private boolean debug;

	ConnectionPool pooldb2;
	ConnectionPool poolSeg;

	private int alertasTotales=0;
	private int totalExecs=0;

	private boolean useCrypto;

	private String keyFilePath;

	/**
	 * Se encarga de configurar los distintos parametros del sink, entre ellos la conexiónes a las bases de datos.  
	 * Estas configuraciones son entregadas a través del archivo de configuración del flujo de flume.  
	 * 
	 * 
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context ctx) {
		loadProperties(ctx);

		if(urlSeg == null || driverSeg == null || usrSeg == null || passSeg == null){
			//System.out.println("Error de Propiedades - Se enviaron Argumentos Invalidos para conexion con BD Seguridad");
			Log.logError("Configure", "Propiedades - Se enviaron Argumentos Invalidos para conexion con BD Mineria");
		}else{

			if(urlISeries == null || driverISeries == null || usrISeries == null || passISeries == null){
				//System.out.println("Error de Propiedades - Se enviaron Argumentos Invalidos para conexion con BD iSeries");
				Log.logError("Configure", "Propiedades - Se enviaron Argumentos Invalidos para conexion con BD iSeries");
			}else{
				//System.out.println("---> AlertaSINK Configurado!!!");
				Log.logInfo("Configure", "---> AlertaSINK Configurado!!!");
			}

		}

	}

	/**
	 * Se encarga de configurar los distintos parametros del sink, entre ellos la conexiónes a las bases de datos.  
	 * Estas configuraciones son entregadas a través del archivo de configuración del flujo de flume.  Los parametros que recibe son los siguientes
	 * 

	 * <li><b>urlSeg</b>: URL de conexion a la base de datos de Mineria.</li>
	 * <li><b>driverSeg</b>: DRIVER para la base de datos de Mineria.</li>
	 * <li><b>usrSeg</b>: usuario de conexión a la base de datos de Mineria.</li>
	 * <li><b>passSeg</b>: Contraseña para la base de datos de Mineria.</li>
	 * 
	 * <li><b>urlISeries</b>: URL de conexion a la base de datos de iSeries-Medellín.</li>
	 * <li><b>driverISeries</b>: DRIVER para la base de datos de iSeries-Medellín.</li>
	 * <li><b>usrISeries</b>: usuario de conexión a la base de datos de iSeries-Medellín.</li>
	 * <li><b>passISeries</b>: Contraseña para la base de datos de iSeries-Medellín.</li>
	 * 
	 * <li><b>useCrypto</b>: Parametro booleano que define si se usa Criptografia para desencriptar las contraseñas.  Valor por defecto = false</li>
	 * <li><b>keyFilePath</b>: En caso de useCrypto = true.  La ruta del archivo que contiene la clave de desencripción</li>
	 * 
	 * <li><b>debug</b>: Boolean que dice sei se deben imprimir mensajes informativos, por defecto en valor = false.</li>
	 * <li><b>numConn</b>: Numero de conexiones para cada pool de conexiones (Mineria e iSeries), Por defecto = 3.</li>

	 * 
	 * 
	 * @param ctx Contexto enviado por el flujo de FLume
	 */
	private void loadProperties(Context ctx) {
		debug = ctx.getBoolean("debug", false);
		urlSeg = ctx.getString("urlSeg");
		driverSeg = ctx.getString("driverSeg");
		usrSeg = ctx.getString("usrSeg");
		passSeg = ctx.getString("passSeg");
		numConn = ctx.getInteger("numConn", 3);

		urlISeries = ctx.getString("urlISeries");
		driverISeries = ctx.getString("driverISeries");
		usrISeries = ctx.getString("usrISeries");
		passISeries = ctx.getString("passISeries");

		useCrypto = ctx.getBoolean("useCrypto", false);

		if(useCrypto){
			keyFilePath = ctx.getString("keyFilePath");
			if(keyFilePath == null || keyFilePath.trim().equals("") ) {
				Log.logError("Configure", "Debe haber una ruta con el KEYFILE cuando se usa la configuración de Criptografía (useCrypto)");
			}else{
				try {
					passSeg = PwDecryptor.decrypt(keyFilePath, passSeg);
				} catch (Exception e) {
					Log.logError("Configure", "Se encontró problema con la desencripción de la contraseña de Mineria.  Mensaje: " + e.getMessage());
				}


				try {
					passISeries = PwDecryptor.decrypt(keyFilePath, passISeries);
				} catch (Exception e) {
					Log.logError("Configure", "Se encontró problema con la desencripción de la contraseña de iSeries.  Mensaje: " + e.getMessage());
				}

			}
		}





		if(debug){
			System.out.println("***** Propiedades Cargadas *****");
			System.out.println("debug : " + debug);


			System.out.println("driverSeg : " + driverSeg);
			System.out.println("urlSeg : " + urlSeg);
			System.out.println("usrSeg : " + usrSeg);
			//System.out.println("passSeg : " + passSeg);

			System.out.println("driverISeries : " + driverISeries);
			System.out.println("urlISeries : " + urlISeries);
			System.out.println("usrISeries : " + usrISeries);
			//System.out.println("passISeries : " + passISeries);

			System.out.println("numConn : " + numConn);

			if(useCrypto){
				System.out.println("useCrypto: " + useCrypto);
				System.out.println("keyFilePath: " + keyFilePath);
			}

			System.out.println("\nSQL-SINK VERSION: " +VERSION+"\n");

			System.out.println("********************************");

		}

	}


	/**
	 * Metodo llamado por el flujo de flume.  Sirve para inicializar los pools de conexiones a las bases de Datos.
	 * 
	 * @see org.apache.flume.sink.AbstractSink#start()
	 */
	@Override
	public void start() {
		try {



			pooldb2 = new ConnectionPool(numConn, numConn+2, driverISeries , urlISeries, usrISeries, passISeries);
			poolSeg = new ConnectionPool(numConn, numConn+2, driverSeg , urlSeg, usrSeg, passSeg);


			//System.out.println(" ---> AlertaSINK Iniciado!!!");
			Log.logInfo("Start", " ---> AlertaSINK Iniciado!!!");

		} catch (ClassNotFoundException e) {
			//System.out.println(" Error Iniciando SINK!!! -> No se encuentra el Driver Especificado: " + e.getMessage());
			Log.logError("Configure", "Iniciando SINK!!! -> No se encuentra el Driver Especificado: " + e.getMessage());
			e.printStackTrace();
		}catch (SQLException e) {
			//System.out.println(" Error Iniciando SINK!!! -> Generando la conexion: " + e.getMessage());
			Log.logError("Configure", "Iniciando SINK!!! -> Generando la conexion: " + e.getMessage());
			e.printStackTrace();
		} 
	}



	/** Metodo invocado en el programa de Flume.  Éste metodo se llama de manera continua por Flume, y sirve para realizar la insercion de los campos en las bases de datos de bloqueo de clave.
	 *  Cada que se ejecuta el método se preparan todos los queries para insertar. <br> 
	 *  Se ejecuta la conexión y se insertan los campos enviados por el flujo.  Cada registro es insertado de manera independiente.
	 * @see org.apache.flume.Sink#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		long start = System.currentTimeMillis();
		totalExecs+=1;
		Status status = null;
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();

		int actual = alertasTotales;

		try {

			//Se trae un evento del Canal.  Es posible que el evento este vacio porque el canal está vacío.
			Event evt = ch.take();
			String ret = null;

			if (evt != null && (new String(evt.getBody())).length() >= 0) {
				//En el caso que el evento exista, se ejecutan las queries enviadas en el evento
				String msg = new String(evt.getBody());
				if(debug){
					//System.out.println("Mensaje =  " + msg );
					Log.logInfo("Process", "Mensaje =  " + msg );
				}
				String[] campos = msg.split(",");
				ret = executeQueries(campos);
				alertasTotales+=1;
			}


			if(debug){
				if(ret != null){
					//System.out.println( ret );
					Log.logInfo("Process", "QueryReturn =  " + ret );
				}

			}

			txn.commit();

			if(actual != alertasTotales){
				status = Status.READY;
			}else{
				status = Status.BACKOFF;
			}


		} catch (Exception e) {
			txn.rollback();
			//System.out.println("Error Insertando Valores de ALertas=" + e.getMessage());
			Log.logError("Process", "Insertando Valores de Alertas: " + e.getMessage() );
			// Log exception, handle individual exceptions as needed

			status = Status.BACKOFF;

			// re-throw all Errors
			/*if (t instanceof Error) {
				throw (Error)t;
			}*/
		} finally {
			txn.close();
		}
		if(debug){

			long totalTime =  System.currentTimeMillis() - start;
			if(actual != alertasTotales){
				//System.out.println( "STATUS: "  + status + ", Ejecuciones: " + totalExecs + ", Alertas Totales: " + alertasTotales + ", Hora: " + getFechaHora() + "\n");
				Log.logInfo("Process", "STATUS: "  + status + ", Ejecuciones: " + totalExecs + ", Alertas Totales: " + alertasTotales
						+ " duracion = " + totalTime + " ms. (" + totalTime/1000 + " s)");
			}

		}
		return status;

	}




	/**
	 * Se encarga de ejecutar los queries y las inserciones a todas las bases de datos.  Se trae una conexión a cada base de datos desde el pool de conexiopnes
	 * luego realiza las inserciones y cierra la conexión con el pool.
	 * @param campos el Array de strings recibido desde Spark
	 * @return el resultado de la ejecución de la inserción de cada BD
	 * @throws Exception En el caso que haya errores en la inserción.
	 */
	private String executeQueries(String[] campos) throws Exception {

		connISeries = pooldb2.getConnection();
		connSeg = poolSeg.getConnection();

		String sql = Queries.bloqueoISeries(campos, 1);  
		String res1 = insertInDB(sql,connISeries,"iSeries-b1");

		//sql = Queries.bloqueoISeries(campos, 3);  
		//String res2 = insertInDB(sql,connISeries,"iSeries-b3");
		String res2 = "BLQ3Deprec";

		sql = Queries.bloqueoDirecto(campos, 1); 
		String res3 = insertInDB(sql,connSeg,"Seguridad-b1");

		//sql = Queries.bloqueoDirecto(campos, 3); 
		//String res4 = insertInDB(sql,connSeg,"Seguridad-b3");
		String res4 = "BDirecto3Deprec";

		sql = Queries.r_RECARGASTIGO(campos);
		String res5 = insertInDB(sql,connSeg,"Reglas_r");

		sql = Queries.g_RECARGASTIGO(campos);
		String res6 = insertInDB(sql,connSeg,"Reglas_g");

		connISeries.close();
		connSeg.close();

		return res1 + "-" + res2 + "-" + res3 + "-" + res4 + "-" + res5 + "-" + res6;
	}


	/**
	 * Genera la inserción de un query en una base de datos específica.
	 * 
	 * @param SQL El query con la inserción específica
	 * @param conn la conexión a la base de datos
	 * @param tipo un identificador de la inserción
	 * @return un resultado de la ejecución
	 * @throws Exception En el caso de que haya errores en la inserción
	 */
	private String insertInDB(String SQL, Connection conn, String tipo) throws Exception {
		Statement stmt;
		String msg = "ERROR";
		try{

			stmt = conn.createStatement();
			int i = stmt.executeUpdate(SQL);  

			msg = tipo + ":" + i;
			//Log.logInfo(msg);

		}catch(SQLException se){
			//Log.logInfo("insertInDB", "STATUS: "  + status + ", Ejecuciones: " + totalExecs + ", Alertas Totales: " + alertasTotales + ", Hora: " + getFechaHora()  );
			throw new Exception("Insertando Valores [insertInDB][" + tipo.toUpperCase() + "] " + se.getMessage() + "SQL:" + SQL);
		}
		//System.out.println("Error Insertando Valores [" + tipo.toUpperCase() + "] " + e.getMessage() );


		return msg;
	}



	/** 
	 * Método ejecutado por Flume.  Se ejecuta al parar el flujo.  Se utiliza en este caso para cerrar las conexiones a las bases de Datos.
	 * 
	 * 
	 * @see org.apache.flume.sink.AbstractSink#stop()
	 */
	@Override
	public void stop () {
		try {
			connSeg.close();
			connISeries.close();
		} catch (SQLException e) {
			//System.out.println("Error Cerrando la conexion: " + e.getMessage());
			Log.logError("Stop", "Error Cerrando la conexion: " + e.getMessage() );
		}

		//System.out.println("---> AlertaSINK Detenido!!!");
		Log.logInfo("Stop", "---> AlertaSINK Detenido!!!" );
	}


}
