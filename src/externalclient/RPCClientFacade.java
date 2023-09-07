package co.bancolombia.flume.externalclient;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * 
 * Clase que funciona como una interfaz RPC para enviar mensajes AVRO a Flume<br><br>
 * 
 * Este es una implemente modificada de un ejemplo que se encuentra originalmente en la documentación de Flume:<br><br>
 * 
 * Flume Developer Guide 1.6<br>
 * https://flume.apache.org/FlumeDeveloperGuide.html<br>
 * 
 * @author rlarios
 *
 */
public class RPCClientFacade {
	private RpcClient client;
	private String hostname;
	private int port;

	/**
	 * Inicializa el cliente RPC para enviar información a flume
	 * @param prop archivo de propiedades para inicializar el cliente.  Debe tener los parámetros de huesped (<b>hostname</b>) y puerto (<b>port</b>) 
	 * del agente de flume que estará escuchando remotamente.
	 */
	public void init(Properties prop) throws Exception{
		// Setup the RPC connection
		//String hostname, int port
		this.hostname = prop.getProperty("hostname");
		this.port = Integer.valueOf(prop.getProperty("port"));

		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		// Use the following method to create a thrift client (instead of the above line):
		// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		System.out.println("Instancia RPC Iniciada " + client.isActive());
	}

	/**
	 * Usa el Cliente para enviar información a Flume 
	 * @param data cadena de caracteres para enviar.
	 */
	public void sendDataToFlume(String data) {
		// Create a Flume Event object that encapsulates the sample data
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

		// Send the event
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
			// Use the following method to create a thrift client (instead of the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}

	/**
	 * Close the RPC connection
	 */
	public void cleanUp() {

		client.close();
	}
}
