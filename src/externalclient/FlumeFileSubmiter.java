package co.bancolombia.flume.externalclient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;





/**
 * Clase principal para el envío de Mensajes a un agente externo de Flume.  La clase recibe en el primer argumento un archivo de propiedades con 
 * los siguientes parámetros:
 * 
 * <br><br>sourceFile=ruta del archivo de texto donde tomará las líneas para enviar.
 * <br>hostname=nombre o dirección IP del servidor de Flume
 * <br>port=puerto al cual se enviarán los mensajes de Flume
 * <br>notifyNum=Número que define cada cuantas lineas mostrará un indicador de estado.
 * 
 * <br><br>La forma de llamar la clase es: 
 * java -jar <jar-file-name>.jar /ruta/del/archivo.properties
 * 
 * @author RLARIOS
 *
 */
public class FlumeFileSubmiter {

	private static Properties prop = new Properties();

	private static BufferedReader br = null;




	/**
	 * Función principal de la clase.  Carga el archivo de propiedades que tiene la información del
	 * archivo fuente y de la información de la conexion al agente externo de Flume.
	 * 
	 * El programa lee linea por linea el archivo fuente y por cada linea leída envia 
	 * el contenido de la linea a Flume
	 * 
	 * @param args en el primer argumento estará la ruta del archivo de propiedades ( args[0] )
	 */

	public static void main(String[] args) {

		long start = System.currentTimeMillis();
		Random randomGenerator = new Random();


		if(args.length == 0){
			System.out.println("Error: Debe Proporcionarse la direccion del archivo de propiedades como argumento");
			System.out.println("uso: java -jar <jar-file-name>.jar \"ruta/del/archivo.properties\"");

		}else if(args[0] == null || args[0] == ""){
			System.out.println("Error: Debe Proporcionarse la direccion del archivo de propiedades como argumento");
			System.out.println("uso: java -jar <jar-file-name>.jar \"ruta/del/archivo.properties\"");

		}else{
			System.out.println("\n == Inicio del Proceso ==");
			System.out.println("Archivo de Propiedades: " + args[0]);
			long i = 1;
			try {
				loadProperties(args[0]);
				int notifyNum = Integer.parseInt(prop.getProperty("notifyNum","100000"));
				br = new BufferedReader(new FileReader(prop.getProperty("sourceFile")));

				RPCClientFacade worker = new RPCClientFacade();
				worker.init(prop);

				String line;

				int seconds = Integer.parseInt(prop.getProperty("randomSeconds","0"));
				System.out.println("Inicio de lectura de archivo fuente \n");
				while( (line = br.readLine()) != null){

					worker.sendDataToFlume(line);

					if(i % notifyNum == 0){
						System.out.println("Envio a flume linea # " + i);
						System.out.println(line);
						System.out.println( ( (i) /  ((System.currentTimeMillis() - start)/1000 ) ) + " mensajes por segundo\n");
					}

					i+=1;

					if(seconds > 0){
						try {
							//Se duerme durante hasta x segundos (dependiendo de lo que manden.
							Thread.sleep(randomGenerator.nextInt(seconds*1000));
						} catch (Exception e) {
							System.out.println("Error en Sleep: " + e.getMessage());
						}
					}
				}
				System.out.println("Fin Lectura de Archivo");

				worker.cleanUp();
				br.close();

			} catch (IOException e) {

				System.out.println("error IO: (fila actual = " + i + "): " + e.getMessage());
			} catch (Exception e1) {
				System.out.println("error Exception: (fila actual = " + i + "): " + e1.getMessage() + " " + e1.getClass());
			} 

			long segs = ( (System.currentTimeMillis() - start) / 1000 ) + 1;  //+1 porque podría ser 0 y no generar DivByZero

			System.out.println("\n == Fin del Proceso ==");
			System.out.println( ( (i) / segs ) + " mensajes por segundo");
			System.out.println("Duracion del proceso: " + segs + " segundos.");

		}

	}

	/**
	 * Carga las propedades enviadas en el la ruta del archivo de propiedades.  
	 * La variable Properties tendra la información de cargar el worker que enviará los mensajes
	 * y el archivo fuente de donde tomará la información.
	 * 
	 * 
	 * @param path ruta del archivo de propiedades
	 * @throws FileNotFoundException En el caso que el archivo de propiedades no se encuentre
	 * @throws IOException En el caso en que haya problemas en la lectura del archivo de propiedades
	 */
	private static void loadProperties(String path) throws FileNotFoundException, IOException {

		prop.load(new FileInputStream(path));

		System.out.println("\nPropiedades cargadas en el Archivo:");
		System.out.println("sourceFile=" + prop.getProperty("sourceFile"));
		System.out.println("notifyNum=" + prop.getProperty("notifyNum"));
		System.out.println("randomSeconds=" + prop.getProperty("randomSeconds"));
		System.out.println("hostname=" + prop.getProperty("hostname"));
		System.out.println("port=" + prop.getProperty("port"));


	}

}
