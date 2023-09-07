package co.bancolombia.flume.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;

import org.apache.flume.source.AbstractPollableSource;

import co.bancolombia.flume.sources.snmp.BatchEvent;


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
 * valores de la tabla.  La clase debe utilizarse para tablas transaccionales con un campo incremental ya sea de fecha o de pk incremental.
 * 
 * 
 * @author rlarios
 *
 */
public class SQLIncrementalSourceTSDB extends AbstractPollableSource implements Configurable{


	private static String url,driver,usr,pwd,query,initialValue,field,limitStatement;
	private static String metricName,metricValue;
	private static String[] tags;
	private static boolean debug = false;

	private static int maxcap , executions = 0 , totalrows = 0;

	private static String LASTDATA = "";

	private static Connection conn = null;

	private int delayQuery = 0;


	@Override
	public void configure(Context ctx) {
		url = ctx.getString("url");
		driver = ctx.getString("driver");
		usr = ctx.getString("usr");
		pwd = ctx.getString("pwd");
		query = ctx.getString("query");
		field = ctx.getString("field");
		delayQuery = ctx.getInteger("delayQuery", 0);
		maxcap = ctx.getInteger("maxcap",100).intValue();
		limitStatement = ctx.getString("limitStatement" , "");

		metricName = ctx.getString("metricName");
		metricValue = ctx.getString("metricValue");
		tags = ctx.getString("tags").split(",");
		debug = ctx.getBoolean("debug",false);


		initialValue = ctx.getString("initialValue");

		System.out.println("=====================");

		System.out.println("CONFIGURACION INICIAL");
		System.out.println("url: " + url);
		System.out.println("driver: " + driver);
		System.out.println("query: " + query);
		System.out.println("field: " + field);
		System.out.println("initialValue: " + initialValue);
		System.out.println("delayQuery: " + delayQuery);
		System.out.println("maxcap: " + maxcap);
		System.out.println("limitStatement: " + limitStatement);

		System.out.println("metricName: " + metricName);
		System.out.println("metricValue: " + metricValue);
		System.out.println("tags: " + ctx.getString("tags"));
		System.out.println("debug: " + debug);


		System.out.println("=====================");




	}



	@Override
	public synchronized void start() {
		try {
			Class.forName(driver);
			try {
				conn = DriverManager.getConnection(url, usr, pwd);
			} catch (SQLException e) {
				System.out.println("Error tomando la conexión (SQLException) : " + url + " " + e.getMessage());
			} 
		} catch (ClassNotFoundException e1) {
			System.out.println("No se encontro el Driver (ClassNotFoundException) - Driver enviado: " + driver + " Mensaje: " + e1.getMessage());
		} 

	}

	@Override
	public synchronized void stop() {
		try {
			conn.close();
		} catch (SQLException e) {
			System.out.println("Error cerrando la conexión (SQLException) : " + url + " " + e.getMessage());
		}
	}


	@Override
	public Status process() throws EventDeliveryException {

		/*
		 * 
		 * Cada que se ejecuta el método se prepara el query para que solamente traiga los últimos valores 
		 * ( CAMPO_INCREMENT > LAST_VALUE )
		 * Se ejecuta la conexión y los resultados se traen y se separan por comas.  
		 * Cada registro es procesado para que sea enviado a través de Flume.
		 * 
		 */
		String tempLastValue = LASTDATA;
		executions += 1;
		long start = System.currentTimeMillis();
		Status status = Status.BACKOFF;
		String q = "";
		PreparedStatement ps;
		ResultSet resultado;

		List<byte[]> eventList = new ArrayList<byte[]>();

		try {

			if(!conn.isValid(1000))
				conn = DriverManager.getConnection(url, usr, pwd);

			q = prepareQuery(query);

			if(debug)
				System.out.println("Query = " + q);


			ps = conn.prepareStatement(q);
			resultado = ps.executeQuery();
			int cols = resultado.getMetaData().getColumnCount();
			int rows = 0;
			while(resultado.next()){

				// SELECT cast(substring(itc.nomdisp,1,5) as int) as codigo_sucursal, horatrn, itc.vlrtran, suc.nombre,  suc.ciudad , suc.departamento, suc.region 

				//FOR OPEN TSDB
				// put nombre_metrica Timestamp valor_metrica tag1=valor tag2=valor

				//Para el caso de este ejemplo la idea es
				//put valor_transaccion 398427528234 500123 codigo=x nombre=x ciudad=m region=y 

				String event = "put " + metricName + " " 
						+ System.currentTimeMillis() + " " 
						+ resultado.getDouble(metricValue) 
						+ getTagInformation(resultado);

				rows += 1;

				for(int i=1;i<=cols;i++){

					String val = resultado.getString(i);

					if(resultado.getMetaData().getColumnName(i).toLowerCase().equals(field.toLowerCase())){
						tempLastValue = val;
					}

				}

				eventList.add(event.getBytes());

				event = "";
				//writeEvent(event);

				LASTDATA = tempLastValue;

				if(rows == maxcap){
					resultado.close();
					break;
				}


			}

			if (!eventList.isEmpty())
				getChannelProcessor().processEvent(BatchEvent.encodeBatch(eventList));

			resultado = null;


			System.out.println("\nEscribio " + rows + " registros. LASTVALUE = " + LASTDATA);
			totalrows += rows;
			if(rows == 0){
				status = Status.BACKOFF;
			}else{
				status = Status.READY;
			}

			//conn.close();


		} catch (SQLException e) {

			System.out.println("Error Ejecutando el Query (SQLException) : " + q + " -- Mensaje: " + e.getMessage());

		} /*catch(EventDeliveryException ev){

			System.out.println("Error Enviando Eventos (EventDeliveryException) : Mensaje: " + ev.getMessage());
		}*/

		System.out.println("Satus : " + status + " Filas Totales: " + totalrows + " Ejecucion # " + executions + " duracion = " + (System.currentTimeMillis()-start)/1000 + "\n");

		if(delayQuery > 0 ){
			try {
				Thread.sleep(delayQuery*1000);
			} catch (InterruptedException e) {
				System.out.println("Error en Procesamiento (_sleep_) - Evento No procesado : " + e.getMessage() );
			} 
		}

		return status;
	}


	/**
	 * @param resultado ResultSet con la información de la fila actual
	 * @return String de la forma " nombreTag1=valorTag1 nombreTag2=valorTag2 .... nombreTagN=valorTagN" 
	 * @throws SQLException cuando hay problemas con el resultSet
	 */
	private String getTagInformation(ResultSet resultado) throws SQLException {
		String ret = "";

		for(String tag : tags){
			ret = ret + " " + tag + "=" + resultado.getString(tag);
		}

		return ret;
	}



	/*
	 * Método que se encarga de registrar el evento en Flume.  Envia una cadena de bytes sacado del string del registro.
	 *
	 * @param registro Registro de la consulta de SQL.

	private void writeEvent(String registro) throws EventDeliveryException{
		Event event = EventBuilder.withBody(registro.getBytes());
		getChannelProcessor().processEvent(event);

	}
	 */

	/**
	 * Prepara el query para que se ejecute con los valores adecuados.  La idea es que siempre se ejecute para que la 
	 * consulta traiga solo los ultimos valores, y solo traiga los registros cambiados.
	 * 
	 * @param _query
	 * @return
	 */
	private String prepareQuery(String _query) {
		/* El formato de la consulta es:
		 * 
		 *  SELECT * FROM nombre_tabla Where CAMPO_INCREMENT > LAST_VALUE.
		 * 
		 */

		String queryRet = "";

		if(LASTDATA == null || LASTDATA.equals("")){
			queryRet = _query.replace("CAMPO_INCREMENT", field).replace("LAST_VALUE", initialValue);
		}else{
			queryRet = _query.replace("CAMPO_INCREMENT", field).replace("LAST_VALUE", LASTDATA);
		}

		return queryRet  + " " + limitStatement;
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
