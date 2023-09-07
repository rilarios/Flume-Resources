package co.bancolombia.flume.externalclient;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Clase de ejemplo para mostrar como se utiliza el conector de Flume. Recibe los datos de conexión y establece
 * una conexion con un agente activo de Flume.  Una vez conectado abre una consola para escribir 
 * mensajes a dicho flujo.<br><br>
 * 
 * Sale del sistema cuando la persona escribe "cancelar" en la consola.
 * 
 * @author rlarios
 *
 */
public class ExampleFlumeConnector {

	public static void main(String[] args) {
		FlumeConnector conn = FlumeConnector.getInstance();
		
		try {
			String server = "Poner ip o direccion de servidor aqui";
			int port = 11001;  //mirar cual es el que es 
			
			FlumeWorker worker = conn.getWorker(server, port);
			
			String line = "";

			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("\nEscriba Mensaje a enviar a FLume (Cancelar para salir): ");
			line = br.readLine();
			
			while(!line.trim().toUpperCase().equals("CANCELAR")){
				worker.sendDataToFlume(line);
				System.out.println("--");
				line = br.readLine();
			}
			
			worker.cleanUp();			
			System.exit(0);
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
