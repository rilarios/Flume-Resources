package co.bancolombia.flume.externalclient;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

/**
 * Clase que administra las conexiones a flujos de Flume Externos.
 * 
 * La clase funciona como un singleton, que establece conexiones a flume a través de "workers", 
 * el cual no es mas que un cliente RPC.
 * 
 * @see co.bancolombia.flume.externalclient.FlumeWorker
 * 
 * @author rlarios
 *
 */
public class FlumeConnector implements Serializable{
	
	private static final long serialVersionUID = 8659747728384699564L;
	private static FlumeConnector instance = null;

	private long startTime;
	
	
	private HashMap<String,FlumeWorker> workers = new HashMap<String,FlumeWorker>();
	
	/**
	 * Constructor del Conector.  Simplemente define cual es la fecha de inicio
	 */
	private FlumeConnector(){
		startTime = System.currentTimeMillis();
	}
	

	
	/**
	 * Trae la instancia unica del conectior de Flume.  Es un método estático para garantizar singleton.
	 * @return instancia de FlumeConnector
	 */
	public static synchronized FlumeConnector getInstance(){
		if(instance == null){
			instance = new FlumeConnector();
		}
		
		return instance;
	}
	
	
	
	/**
	 * Genera una instancia de un Worker de FLume. en el caso que ya exista una instancia del worker la devuelve.
	 * De lo contrario construye una nueva.
	 * 
	 * @param host Nombre o direccion IP del Host de flume
	 * @param port Puerto de Flume
	 * @return Una instancia del worker de acuerdo al puerto y dirección.
	 * @throws Exception En el caso que haya errores en la generación del Worker.
	 */
	public FlumeWorker getWorker(String host, int port) throws Exception{
		Properties prop = new Properties();
		prop.setProperty("hostname", host);
		prop.setProperty("port", "" +port);
		return getWorker(prop);
	}
	
	
	/**
	 * Genera una instancia de un Worker de FLume. en el caso que ya exista una instancia del worker la devuelve.
	 * De lo contrario construye una nueva.
	 * 
	 * @param prop archivo de propiedades del worker que se desea.  con propiedades de hostname y port.
	 * @return Una instancia del worker de acuerdo al puerto y dirección.
	 * @throws Exception En el caso que haya errores en la generación del Worker.
	 */
	private FlumeWorker getWorker(Properties prop) throws Exception{
		String key = prop.getProperty("hostname") + ":" + prop.getProperty("port");
		FlumeWorker worker = workers.get(key);
		if(worker == null){
			worker = new FlumeWorker(prop);
			workers.put(key, worker);
		}
		
		return worker;
	}
	
	/**
	 * Limpia los workers del conector.  Hace una salida limpia de cada worker.
	 */
	public void removeWorkers(){
		for(String key  : workers.keySet()){
			workers.get(key).cleanUp();
			workers.remove(key);
		}
	}
	
	/**
	 * Genera el status del conector.  Genera un mensaje indicando el número de workers activos y 
	 * el total de segundos que se ha encontrado activo.
	 * @return
	 */
	public String getStatus(){
		return "There are " + workers.size() + "workers active!. \n"
				+ "FlumeConnector working for " + ((System.currentTimeMillis() - startTime)/1000) + " seconds!.";
	}
			
			

}
