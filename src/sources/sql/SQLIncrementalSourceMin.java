package co.bancolombia.flume.sources.sql;


import java.sql.Connection;
//import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;


import java.util.Date;
import java.util.List;

import co.bancolombia.flume.util.Log;

import co.bancolombia.flume.util.PwDecryptor;
import co.bancolombia.hbase.base.HBaseConnector;


//import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;



/**
 * Clase que permite realizar consultas incrementales sobre una base de datos y procesar cada registro como un evento de Flume.
 * Esta clase es llamada por el agente de flume como una fuente personalizada (Custom Source).<br><br>
 * 
 * La clase carga los parametros de conexion y flume se encarga de llamar la clase cada cierto tiempo (500 ms) y ejecutar el método process() el cual
 * realiza la conexión y ejecuta la consulta.  La consulta debe estar en el formato: <br><br>
 * 
 * SELECT * FROM nombre_tabla Where CAMPO_INCREMENT > LAST_VALUE.<br><br>
 * 
 * Lo que hace la clase es que guarda el último valor guardado y por lo tanto cada vez que se ejecute el metodo process() solo traerá los ultimos
 * valores de la tabla.  La clase debe utilizarse para tablas transaccionales con un campo incremental ya sea de fecha o de pk incremental.<br><br>
 * 
 * <b>ATENCIIÓN:</b>  Esta es una clase modificada de SQLIncrementalSource, sin embargo tiene una modificación especial, en el caso que
 * las transacciones lleguen con un valor en el futuro, impidiendo que transacciones que lleguen no se inserten (porque el campo incremental es mayor 
 * que la transaccion que llega.
 * 
 * @see co.bancolombia.flume.sources.sql.SQLIncrementalSource
 * 
 * @author rlarios
 *
 */
public class SQLIncrementalSourceMin extends AbstractPollableSource implements Configurable{



	// Ultimos cambios - Modificación de Loggeo
	//                 - Cambio Mensajes de Loggeo
	//                 - Se añade soporte para criptografía
	//  			   - Mejora HBase mensaje
	//  			   - Spacing
	private String VERSION = "1.6 Mineria [20170915]";
	//variables de Contexto (Se envian en la configuración)

	private String url,driver,usr,pwd,query,initialValue,field,limitStatement;
	private boolean debug = false;
	private int maxcap;
	private String LASTDATA = "";
	private String sqlSourceName = "";
	private ArrayList<Event> batch;

	//Variables de la entrada


	private Connection conn = null;
	private int delayQuery = 0;

	private boolean isCalculated = false, putTimestamp = false;
	private int columnField = -1;
	private int executions = 0 , totalrows = 0 , connectionPool = 0;

	private ConnectionPool bds = null;

	private Boolean useHBase;

	private HBaseConnector hbase;

	private boolean useCrypto;
	private String keyFilePath;


	private static SimpleDateFormat fmtHora = new SimpleDateFormat("HHmmss");
	private static SimpleDateFormat fmtfecha = new SimpleDateFormat("yyyyMMdd");

	/**
	 * Genera un string con la hora actual 
	 * @return String de la fecha/Hora actual en formato HHmmss
	 */
	private static String getHora() {
		Date now = Calendar.getInstance().getTime();
		return fmtHora.format(now);
	}

	/**
	 * Genera un string con fecha actual.
	 * @return String de la fecha/Hora actual en formato yyyyMMdd
	 */
	private static String getFecha() {
		Date now = Calendar.getInstance().getTime();
		return fmtfecha.format(now);
	}


	/**
	 * Metodo invocado en el programa de Flume.  Al método se le pasa un objeto Context el cual contiene las variables de configuración del objeto.<br>
	 * Estos parámetros de configuración son entregados al flujo de flume en el archivo de configuración que se le entrega al ser ejecutado.<br>
	 * Los parámetros que pueden definirse son los siguientes:
	 * <li><b>url</b>: String de conexión a la base de datos relacional.</li>
	 * <li><b>driver</b>: String de definición del driver usado para conectarse a la base de datos.</li>
	 * <li><b>useCrypto</b>: Parametro booleano que define si se usa Criptografia para desencriptar la contraseña.  Valor por defecto = false</li>
	 * <li><b>usr</b>: String con el usuario de conexión a la base de datos.</li>
	 * <li><b>pwd</b>: String con la contraseña de conexión a la base de datos.</li>
	 * <li><b>keyFilePath</b>: En caso de useCrypto = true.  La ruta del archivo que contiene la clave de desencripción</li>
	 * <li><b>query</b>: Query a ejecutarse en cada ejecución del flujo. Debe estar en formato SELECT * FROM nombre_tabla Where CAMPO_INCREMENT > LAST_VALUE. (Y ordenada por el campo increment)</li>
	 * <li><b>field</b>: Campo por el cual hara la validación del campo incremental en el query. (MUY IMPORTANTE)  </li>
	 * <li><b>delayQuery</b>: Numero de segundos que espera el proceso antes de ejecutarse de nuevo.  Por defecto tiene un valor de 0</li>
	 * <li><b>maxcap</b>: Número de registros que recibe antes de forzar una nueva ejecución.  Número a tener en cuenta para la capacidad del canal.</li>
	 * <li><b>debug</b>: boolean que Permite lanzar mensajes de error de seguimiento en caso de ser necesario.  Por defecto es false.</li>
	 * <li><b>connectionPool</b>: Número de conexiones a crear en el pool de conexiones. Valor por defecto = 3.</li>
	 * <li><b>putTimestamp</b>: Boolean que indica se al registro enviado se le debe poner un timestamp de flume al final, de manera que se indique el momento de ejecución del proceso por registro.  Valor por defecto = false</li>
	 * <li><b>sqlSourceName</b>: Nombre que se le da al flujo con el fin de identificarlo en el Log. Si no se especifica la clase define uno por defecto con un timestamp</li>
	 * <li><b>useHBase</b>: Parametro booleano que define si se usa HBase para guardar el último valor a leer.  Valor por defecto = false</li>
	 * <li><b>zooServer</b>: En caso de useHBase = true.  El valor del servidor de Zookeper para encontrar el servicio de HBase</li>
	 * <li><b>HBasePort</b>: En caso de useHBase = true.  El valor del puerto de Zookeper para encontrar el servicio de HBase</li>
	 * <li><b>expirySeconds</b>: En caso de useHBase = true.  Valor en segundos en cache de los datos.  Valor por defecto = 1 </li>
	 * <br>
	 * @see org.apache.flume.source.BasicSourceSemantics#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context ctx) {
		url = ctx.getString("url");
		driver = ctx.getString("driver");
		usr = ctx.getString("usr");
		pwd = ctx.getString("pwd");
		useCrypto = ctx.getBoolean("useCrypto", false);
		
		if(useCrypto){
			keyFilePath = ctx.getString("keyFilePath");
			if(keyFilePath == null || keyFilePath.trim().equals("") ) {
				Log.logError("Configure", "Debe haber una ruta con el KEYFILE cuando se usa la configuración de Criptografía (useCrypto)");
			}else{
				try {
					pwd = PwDecryptor.decrypt(keyFilePath, pwd);
				} catch (Exception e) {
					Log.logError("Configure", "Se encontró problema con la desencripción de la contraseña.  Mensaje: " + e.getMessage());
				}
			}
		}
		
		
		
		query = ctx.getString("query");
		field = ctx.getString("field");
		delayQuery = ctx.getInteger("delayQuery", 0);
		maxcap = ctx.getInteger("maxcap",100).intValue();
		debug = ctx.getBoolean("debug", false);
		connectionPool = ctx.getInteger("connectionPool",3);
		limitStatement = ctx.getString("limitStatement" , "");
		putTimestamp = ctx.getBoolean("putTimestamp" , false);

		useHBase = ctx.getBoolean("useHBase" , false);

		if(useHBase){
			sqlSourceName = ctx.getString("sqlSourceName" );
			if(sqlSourceName == null || sqlSourceName.trim().equals("") ) {
				Log.logError("Configure", "No debe haber un nombre de proceso vacio cuando se usa la configuración de HBase (useHBase)");
			}else{
				try {

					initializeHBaseInstance(ctx);
					initialValue = hbase.getValue("last_values_flume", sqlSourceName, "cfValores", "lastValue");
					if(initialValue == null){
						initialValue = "";
					}
				} catch (Exception e) {
					String tipo = "[" + e.getClass().getName() + "]";
					
					Log.logError(tipoMsg("Configure"), tipo + " Problema Conectando o trayendo datos de HBase. Mensaje: " + e.getMessage() );
					e.printStackTrace(System.err);

				}
			}

		}else{
			//Si no usa HBase se trae los valores del archivo de configuración
			sqlSourceName = ctx.getString("sqlSourceName" , getSQLName());
			initialValue = ctx.getString("initialValue");
		}



		//Se imprimen los parámetros de configuración (incluyendo los que no fueron enviados por el archivo, es decir los que se encuentran por defecto. 
		System.out.println("=====================");

		System.out.println("CONFIGURACION INICIAL\n");
		System.out.println("url: " + url);
		System.out.println("driver: " + driver);
		System.out.println("query: " + query);
		System.out.println("field: " + field);
		System.out.println("initialValue: " + initialValue);
		System.out.println("delayQuery: " + delayQuery);
		System.out.println("maxcap: " + maxcap);
		System.out.println("limitStatement: " + limitStatement);
		System.out.println("connectionPool: " +connectionPool);
		System.out.println("putTimestamp: " +putTimestamp);
		System.out.println("debug: " +debug);
		System.out.println("sqlSourceName: " +sqlSourceName);
		System.out.println("useHBase: " +useHBase);

		if(useHBase){
			System.out.println("");
			System.out.println("zooServer: " +ctx.getString("zooServer"));
			System.out.println("zooPort: " +ctx.getInteger("zooPort"));
			System.out.println("expirySeconds: " +ctx.getInteger("expirySeconds",1));
		}
		
		if(useCrypto){
			System.out.println("keyFilePath: " + keyFilePath);
		}


		System.out.println("\nSQL-SOURCE-FIXMINERIA VERSION: " +VERSION+"\n");

		System.out.println("=====================");

		try {
			bds = configureDataSource();
		} catch (ClassNotFoundException e) {
			//System.err.println(tipoMsg("Configure") + "ERROR CARGANDO POOL DE CONEXIONES ABORT ABORT!!!!: " +VERSION+"\n" + e.getMessage());
			Log.logError(tipoMsg("Configure"), "PANIC! CARGANDO POOL DE CONEXIONES ABORT ABORT!!!!: " +VERSION+ ", Mensaje: " + e.getMessage());
		} catch (SQLException e) {
			Log.logError(tipoMsg("Configure"), "PANIC! CARGANDO POOL DE CONEXIONES ABORT ABORT!!!!: " +VERSION+ ", Mensaje: " + e.getMessage());
		}

	}


	/**
	 * Genera una instancia de HBase en el caso que el parametro de LastValue queremos que sea controlado por HBase
	 * @param ctx Contexto de flume que contiene los parametros de conexión a HBase
	 */
	private void initializeHBaseInstance(Context ctx) {
		String zooServer = ctx.getString("zooServer");
		int port = ctx.getInteger("zooPort");
		int expirySeconds = ctx.getInteger("expirySeconds",1);
		hbase = HBaseConnector.getInstance(zooServer, port, expirySeconds);
	}

	/**
	 * Genera un nombre genérico para el Agente en caso que no sea enviado en el archivo de configuración
	 * @return un string de la forma "SQLAGENT-8210928109328"
	 */
	private String getSQLName() {
		return "SQLAGENT-" + System.currentTimeMillis();
	}


	/**
	 * Genera un ConnectionPool (Pool de conexiónes a base de datos) con las configuraciones entregadas en el metodo Configure
	 * @return un ConnectionPool para ser usado en el método process
	 * @throws ClassNotFoundException En el caso que el driver de conexión no sea encuentre en los jars del classpath (Por ejemplo que los jars del drier de SQL Server no se hayan incluido)
	 * @throws SQLException En el caso que haya un error en la conexión al servidor de Base de Datos.
	 */
	private ConnectionPool configureDataSource() throws ClassNotFoundException, SQLException {
		ConnectionPool bds = new ConnectionPool(connectionPool,connectionPool + 2 , driver, url, usr, pwd);
		return bds;
	}


	/**
	 * Metodo invocado en el programa de Flume.  El método se ejecuta al iniciar el flujo y sirve para hacer los inicios de sesión que sean necesarios<br>
	 * En este caso se usa para traer una conexión del pool.
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#start()
	 */
	@Override
	public synchronized void start() {

		//System.out.println("Starting Custom SQL AGENT");
		Log.logInfo(tipoMsg("Start"), "Starting Custom SQL AGENT");
		try {
			conn = bds.getConnection();
		} catch (SQLException e) {
			//System.err.println(tipoMsg("Start") + "Error tomando la conexión (SQLException) : " + url + " " + e.getMessage());
			Log.logError(tipoMsg("Start"), "Problema tomando la conexión (SQLException) : " + url + ", Mensaje: " + e.getMessage());
		} 


	}


	/**
	 * Metodo invocado en el programa de Flume.  El método se ejecuta al parar el flujo y sirve para hacer los cierres de sesión que sean necesarios<br>
	 * En este caso se usa para cerrar todas las conexiones al pool.
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#stop()
	 */
	@Override
	public synchronized void stop() {

		try {
			bds.closeAllConnections();
		} catch (SQLException e) {
			//System.err.println(tipoMsg("Stop") + "Error Deteniendo y Cerrando la conexión (SQLException) : " + e.getMessage());
			Log.logError(tipoMsg("Stop"), "Deteniendo y Cerrando la conexión (SQLException) , Mensaje: " + e.getMessage());
		}

	}

	/**
	 * Metodo invocado en el programa de Flume.  Éste metodo se llama de manera continua por Flume, y sirve para realizar la
	 *  consulta de datos a fuentes externas.  Cada que se ejecuta el método se prepara el query para que solamente traiga los 
	 *  últimos valores ( CAMPO_INCREMENT > LAST_VALUE ).  <br> 
	 *  Se ejecuta la conexión y los resultados se traen y se separan por comas.  Cada registro es procesado para que sea enviado
	 *  a través de Flume.
	 *  
	 *  En el caso de que la ultima hora traida sea mayor a 5 minutos en el futuro (Hora trx - Hora actual) >= 300 se cambia 
	 *  la última hora traida por la hora actual menos un minuto (por si las moscas).
	 *  
	 *          _,_           
	 *       ._(@I@)_.      
	 *      .--{___}--.    
	 *      .-/  Y  \-.    
	 *       /   |   \       
	 *       \__/-\__/     
	 *
	 *  
	 *  @throws EventDeliveryException En el caso que haya un error en el envio de eventos al canal de flume.
	 * 
	 * @see org.apache.flume.source.AbstractPollableSource#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {


		executions += 1;
		long start = System.currentTimeMillis();
		Status status = Status.BACKOFF;
		String q = "";
		PreparedStatement ps;
		ResultSet resultado;


		int rows = 0;

		try {

			if(delayQuery > 0 ){
				try {
					Thread.sleep(delayQuery*1000);
				} catch (InterruptedException e) {
					//System.err.println(tipoMsg("Process") + "Error en Procesamiento (_sleep_) - Evento No procesado : " + e.getMessage() );
					Log.logError(tipoMsg("Process"), "Problema durante hilo dormido (_sleep_) - Evento No procesado : " + e.getMessage());
				} 
			}

			/*if(conn.isClosed()) */
			conn = bds.getConnection();

			q = prepareQuery(query);

			ps = conn.prepareStatement(q);
			resultado = ps.executeQuery();
			int cols = resultado.getMetaData().getColumnCount();


			String timestamp = "";

			String compareField = field.toLowerCase();

			if(putTimestamp){
				Date now = Calendar.getInstance().getTime();
				SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
				timestamp = format.format(now);

			}

			batch = new ArrayList<Event>();

			while(resultado.next()){
				rows += 1;
				StringBuffer col = new StringBuffer("");

				for(int i=1;i<=cols;i++){

					String val = resultado.getString(i);

					// esto trata de conseguir el último valor de comparación lo más eficiente posible
					// no lo he medido pero confío en mis tripas.
					if(isCalculated && columnField == i){
						LASTDATA = val;
					}else{
						if(!isCalculated && resultado.getMetaData().getColumnName(i).toLowerCase().equals(compareField)){
							isCalculated = true;
							columnField = i;
							LASTDATA = val;
						}
					}

					//limpiamos el valor (habrá algun valor válido que tenga comas en la mitad a diferencia de strings? jum
					if(val != null) { val = val.trim().replaceAll("," , " ").replaceAll("\n", " ").replaceAll("\r", " ");}
					col.append(val).append(",");
				}

				if(putTimestamp){
					col.append(timestamp).append(",");
				}


				String registro = col.substring(0, col.length() - 1);

				//writeEvent(registro);
				batch.add(EventBuilder.withBody(registro.getBytes()));

				//LASTDATA = tempLastValue;

				if(rows == maxcap){
					resultado.close();
					break;
				}
			}

			if(batch.size() > 0 ){
				addEventstoChannel(batch);
				//System.out.println("Escribio " + rows + " registros. LASTVALUE = " + LASTDATA);
				Log.logInfo(tipoMsg("Process"), "Escribio " + rows + " registros. LASTVALUE = " + LASTDATA);
			}



			totalrows += rows;

			resultado.close();
			ps.close();

			if(rows == 0){
				status = Status.BACKOFF;
			}else{
				status = Status.READY;
			}


			// En el caso de que la ultima hora traida sea mayor a 5 minutos en el futuro (Hora trx - Hora actual) >= 300 se cambia 
			// la última hora traida por la hora actual menos un minuto (por si las moscas).
			int hora = Integer.parseInt(LASTDATA.substring(8, LASTDATA.length()));
			String Horact = getHora();
			int HorACT = Integer.parseInt(Horact);
			if((hora - HorACT ) >= 300){
				//String last = LASTDATA;
				int nh = HorACT - 100;
				LASTDATA = getFecha() + (getHoraNueva(nh)) ;

				//System.out.println("LASTVALUE incongruente LASTVALUE = " + last + ", nuevo= " + LASTDATA);
				status = Status.BACKOFF;
			}
				
			if(useHBase){
				hbase.putValue("last_values_flume", sqlSourceName, "cfValores", "lastValue", LASTDATA.getBytes());
			}


		} catch (SQLException e) {

			//System.err.println(tipoMsg("Process") + "Error Ejecutando el Query (SQLException) : " + q + " -- Mensaje: " + e.getMessage());
			Log.logError(tipoMsg("Process"), "Problema Ejecutando el Query (SQLException) : " + q + " -- Mensaje: " + e.getMessage() );

		} catch(EventDeliveryException ev){

			//System.err.println(tipoMsg("Process") + "Error Enviando Eventos (EventDeliveryException) : Mensaje: " + ev.getMessage());
			Log.logError(tipoMsg("Process"), "Enviando Eventos (EventDeliveryException) : Mensaje: " + ev.getMessage() );
		} catch(Exception x){
			//System.err.println(tipoMsg("Process") + "Error General (Exception) : " + x.getMessage());
			Log.logError(tipoMsg("Process"), "Problema General (Exception) : " + x.getMessage() );
		}

		bds.returnConnectionToPool(conn);

		long totalTime =  System.currentTimeMillis() - start;

		if(rows > 0){
			//System.out.println(tipoMsg("Process") + "Satus : " + status + " Filas Totales: " + totalrows + " Ejecucion # " + executions + " duracion = " + totalTime + " ms. (" + totalTime/1000 + " s)., \n");
			Log.logInfo(tipoMsg("Process"),"Satus : " + status + " Filas Totales: " + totalrows + " Ejecucion # " + executions + " duracion = " + totalTime + " ms. (" + totalTime/1000 + " s).");
		}

		return status;
	}

	/**
	 * Se trae una hora nueva de acuerdo al formato de HHMMSS
	 * 
	 * @param nh la hora actual
	 * @return la hoira con un cero adelante de ser necesario
	 */
	private String getHoraNueva(int nh) {
		if(nh < 100000){
			return "0" + nh;
		}else{
			return "" + nh;
		}
	}

	/**
	 * Método que se encarga de registrar los eventos en Flume.  Envia una lista de eventos generados por cada string del registro.
	 *
	 * @param events Lista de eventos a enviar
	 */
	private void addEventstoChannel(List<Event> events) throws EventDeliveryException{
		//System.out.println("Tamaño de Batch a enviar a Flume: " + events.size());
		if( events.size()>0 ){
			getChannelProcessor().processEventBatch(events);
		}


	}



	/**
	 * Método que se encarga de registrar el evento en Flume.  Envia una cadena de bytes sacado del string del registro.
	 *
	 * @param registro Registro de la consulta de SQL.

	private void writeEvent(String registro) throws EventDeliveryException{
		Event event = EventBuilder.withBody(registro.getBytes());
		getChannelProcessor().processEvent(event);

	}*/


	/**
	 * Prepara el query para que se ejecute con los valores adecuados.  La idea es que siempre se ejecute para que la 
	 * consulta traiga solo los ultimos valores, y solo traiga los registros cambiados.<br><br>  
	 * 
	 * La primera vez que se ejecuta toma el valor de initialValue enviado en el archivo de configuración, de lo contrario toma el ultimo valor encontrado en la anterior ejecución
	 * 
	 * @param _query el query a preparar.  Enviado en el archivo de configuración
	 * @return El query preparado reemplazando los campos del query.
	 */
	private String prepareQuery(String _query) {
		/* El formato de la consulta es:
		 * 
		 *  SELECT * FROM nombre_tabla Where CAMPO_INCREMENT > LAST_VALUE.
		 * 
		 */

		String queryRet = "";

		if(LASTDATA == null || LASTDATA.trim().equals("")){
			queryRet = _query.replace("CAMPO_INCREMENT", field).replace("LAST_VALUE", initialValue);
		}else{
			queryRet = _query.replace("CAMPO_INCREMENT", field).replace("LAST_VALUE", LASTDATA);
		}

		return queryRet + " " + limitStatement;
	}


	/**
	 * Genera un Header para el mensaje a escribir.  Usado en los System.out.println() del programa
	 * 
	 * @param tipo un String que identifica el tipo de mensaje a enviar
	 * @return Retorna un header del tipo [NOMBREFLUJO-TIPO_MENSAJE]
	 */
	private String tipoMsg(String tipo){
		return sqlSourceName + "-" + tipo;

	}



	@Override
	protected Status doProcess() throws EventDeliveryException {
		return process();
	}

	@Override
	protected void doConfigure(Context arg0) throws FlumeException {
		this.configure(arg0);

	}

	@Override
	protected void doStart() throws FlumeException {
		this.start();

	}

	@Override
	protected void doStop() throws FlumeException {
		this.stop();

	}



}
