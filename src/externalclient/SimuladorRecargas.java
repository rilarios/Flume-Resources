package co.bancolombia.flume.externalclient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * Lee un archivo de Texto enviado por parámetros y envía linea por línea mensajes a Flume.<br>
 * Especifica el numero de milisegundos antes de enviar el próximo mensaje<br>
 * 
 * 
 * Parámetros: archivo servidor:puerto<br>
 * uso: java -jar numbreJar archivo servidorFlume:puerto
 * 
 * @author rlarios
 *
 */
public class SimuladorRecargas {

	public static void main(String[] args) {

		if(args.length < 2){
			System.err.println("Numero de Parámetros incorrecto\nuso: java -jar numbreJar archivo servidorFlume:puerto [ms]\n");
		}else{

			FlumeConnector conn = FlumeConnector.getInstance();

			try {
				String path = args[0];
				String serverFull = args[1];
				int ms = 0;
				try{
					ms = Integer.parseInt(args[2]);
				}catch(Exception e){
					// No se enviaron milisegundos
					ms = 0;
				}


				String server = serverFull.split(":")[0];
				int port = Integer.parseInt(serverFull.split(":")[1]);

				FlumeWorker worker = conn.getWorker(server, port);

				String line = "";

				BufferedReader br = getLectorEncoding(path , "UTF-8");
				
				long start = System.currentTimeMillis();
				System.out.println("\nInicio de Simulador : " + start);
				int i = 1;
				
				while( (line = br.readLine()) != null){
					
					if (i % 100 == 0){
						System.out.println("linea " + i + ": " + line);
					}
					
					worker.sendDataToFlume(line);
					
					if(ms > 0){
						Thread.sleep(ms);
					}
					
					i+=1;
				}



				worker.cleanUp();			
				System.exit(0);

			} catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}

		}



	}

	public static BufferedReader getLectorEncoding(String path,String encoding) throws UnsupportedEncodingException, FileNotFoundException{
		InputStreamReader a = new InputStreamReader(
				new FileInputStream(path), encoding);
		return new BufferedReader(a);
	}

}
