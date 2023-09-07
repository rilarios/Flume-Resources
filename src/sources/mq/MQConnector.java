package co.bancolombia.flume.sources.mq;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;

import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import co.bancolombia.flume.util.Log;


public class MQConnector extends AbstractPollableSource implements Configurable {


	
	// MQ fields
	//private MQConnectionManager conn;
	private MQQueueManager  qmgr;
	private MQQueue queue ;


	private String qmgrName = "";
	private String queueName = "";

	private int executions = 0;
	private int delayQuery = 0;
	private int totalrows = 0;
	private String hostname;
	private String channel;
	private int port;
	private String user;
	private String password;
	private String cipherSuite;
	private Boolean sslFipsRequired;
	private Boolean SSL;
	private String javaKeyStore;
	private String keyStorePass;
	private String IBMCipherMappings;
	private boolean printSupportedCiphers;
	private String javaTrustStore;
	private String protocol;
	private Integer maxBatch;

	@Override
	public void configure(Context ctx) {

		int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_OUTPUT;

		//int otherOptions =  MQConstants.MQOO_INPUT_SHARED | MQConstants.MQGMO_BROWSE_FIRST | MQConstants.MQGMO_BROWSE_NEXT ;

		Log.logInfo("Configure", MQConstants.MQOO_INPUT_AS_Q_DEF + "," + MQConstants.MQOO_OUTPUT + "," + (MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_OUTPUT));
		//System.out.println();



		qmgrName = ctx.getString("qmgrName");
		queueName = ctx.getString("queueName");
		hostname = ctx.getString("hostname");
		port = ctx.getInteger("port");
		channel = ctx.getString("channel");
		user = ctx.getString("user");
		password = ctx.getString("password");
		SSL = ctx.getBoolean("SSL",false);

		cipherSuite = ctx.getString("cipherSuite");
		sslFipsRequired = ctx.getBoolean("sslFipsRequired",true);
		javaKeyStore = ctx.getString("javaKeyStore");
		javaTrustStore = ctx.getString("javaTrustStore");

		keyStorePass = ctx.getString("keyStorePass");
		IBMCipherMappings = ctx.getString("IBMCipherMappings","false");
		printSupportedCiphers = Boolean.valueOf(ctx.getString("printSupportedCiphers","false"));

		maxBatch = ctx.getInteger("maxBatch",100);

		protocol = ctx.getString("protocol","TLSv1");

		//Hashtable<String,String> prop = new Hashtable<String,String>();

		System.out.println("\n***** Propiedades Cargadas *****");
		System.out.println("hostname: " + hostname);
		System.out.println("port: " + port);
		System.out.println("channel: " + channel);
		System.out.println("qmgrName: " + qmgrName);
		System.out.println("queueName: " + queueName);
		System.out.println("user: " + user);
		System.out.println("password: " + password);
		System.out.println("maxBatch: " + maxBatch);
		System.out.println("SSL: " + SSL);

		if(SSL){
			System.out.println("\n--> Security Properties <--");
			System.out.println("sslFipsRequired: " + sslFipsRequired);
			System.out.println("cipherSuite: " + cipherSuite);
			System.out.println("javaKeyStore: " + javaKeyStore);
			System.out.println("javaTrustStore: " + javaTrustStore);
			System.out.println("keyStorePass: " + keyStorePass);
			System.out.println("protocol: " + protocol);
			System.out.println("IBMCipherMappings: " + IBMCipherMappings);
			System.out.println("printSupportedCiphers: " + printSupportedCiphers + "\n");

			//configureSSLContext();


		}

		System.out.println("********************************\n");

		// ACTUAL CONFIGURATION
		try {
			MQEnvironment.hostname = hostname;
			MQEnvironment.channel = channel;
			MQEnvironment.port = port;
			MQEnvironment.userID = user;
			MQEnvironment.password = password;

			if(SSL){

				MQEnvironment.sslCipherSuite = cipherSuite;
				MQEnvironment.sslFipsRequired = sslFipsRequired;

				//System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", IBMCipherMappings);

				System.setProperty("javax.net.ssl.keyStore", javaKeyStore);
				System.setProperty("javax.net.ssl.trustStore", javaTrustStore);				
				System.setProperty("javax.net.ssl.trustStorePassword", keyStorePass);
				MQEnvironment.sslSocketFactory = getSocketFactory();

				System.out.println("ICM: " + System.getProperty("com.ibm.mq.cfg.useIBMCipherMappings"));

				if(printSupportedCiphers){
					printSupportedCiphers();
				}

			}



			//prop.put(CMQC.SSL_FIPS_REQUIRED_PROPERTY,String.valueOf(sslFipsRequired));
			//prop.put(CMQC.SSL_CIPHER_SUITE_PROPERTY,cipherSuite);

			//MQEnvironment.properties = prop;		

			qmgr = new MQQueueManager(qmgrName);
			queue = qmgr.accessQueue(queueName, openOptions);

		} catch (MQException e) {
			System.err.println("[Configure] Error - Configuración de Conexion a MQ : " + e.getMessage() + "\nCAUSE:" + e.getCause() + "\nSTACK:");
			e.printStackTrace();
		}
	}



	private Object getSocketFactory() {
		SSLContext sslc = null;

		try {

			KeyStore ks = KeyStore.getInstance("JKS");
			KeyStore ts = KeyStore.getInstance("JKS");
			
			Log.logInfo("getSocketFactory", "KeyStore=" + ks + ", TrustStore=" + ts);
			//Log.logInfo("getSocketFactory", "pwd=" + keyStorePass );
			//Log.logInfo("getSocketFactory", "KS=" + javaKeyStore );
			//Log.logInfo("getSocketFactory", "TS=" + javaTrustStore );
			
			ks.load(new FileInputStream(javaKeyStore), keyStorePass.toCharArray());	
			ts.load(new FileInputStream(javaTrustStore), null);

			TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); 
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

			tmf.init(ts);
			kmf.init(ks, keyStorePass.toCharArray());

			sslc = SSLContext.getInstance(protocol);	
			sslc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);		


		} catch (Exception e) {
			Log.logError("getSocketFactory", "SSL , class: " + e.getClass().getName() + ", Mensaje: " + e.getMessage());
			//System.out.println();
			e.printStackTrace();
		}
		return sslc.getSocketFactory();
	}







	@Override
	public synchronized void start() {
		//conn = MQConnectionManager.getInstance();
	}

	@Override
	public synchronized void stop() {
		try {
			//conn.disconnect(qmgr);
		} catch (Exception e) {
			Log.logError("Stop", "Desconeccion conn.disconnect(qmgr) : " + e.getMessage());
		}
	}


	/**
	 * 
	 * (non-Javadoc)
	 * @see org.apache.flume.source.AbstractPollableSource#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		long start =  System.currentTimeMillis();
		ArrayList<Event> batch = new ArrayList<Event>();
		Status status = Status.BACKOFF;
		executions  += 1;

		try {


			for(int i=0; i<maxBatch;i++){
				MQGetMessageOptions gmo = new MQGetMessageOptions();
				MQMessage theMessage    = new MQMessage();

				queue.get(theMessage,gmo);
				int length = theMessage.getMessageLength();
				String elString = theMessage.readStringOfCharLength(length);


				//System.out.println("The message is: " + elString);

				batch.add(EventBuilder.withBody(elString.getBytes()));

				totalrows+=1;
			}
			status = Status.READY;


		} catch (MQException e1) {
			if(e1.getReason() != 2033){
				//Si el Error es 2033 es porque se llega al final de la cola
				// Solo en otros casos se pinta el stacktrace
				Log.logError("Process", "Error en Procesamiento (MQEXCEP) - Evento No procesado : " + e1.getMessage());
				e1.printStackTrace(System.err);

			}

			status = Status.BACKOFF;

		}  catch (IOException e) {
			Log.logError("Process", "Error en Procesamiento (IOEXCEP) - Evento No procesado" + e.getMessage());
			e.printStackTrace(System.err);
			status = Status.BACKOFF;

		} catch (Exception ge){
			Log.logError("Process", "Error en Procesamiento (GENEXCEP) - Eventos No procesado" + ge.getMessage());
			ge.printStackTrace(System.err);
			status = Status.BACKOFF;
		}

		int filasProcesadas = batch.size();

		long totalTime =  System.currentTimeMillis() - start;

		if(filasProcesadas > 0){
			addEventstoChannel(batch);
			Log.logInfo("Process", "Satus : " + status + " | Mensajes: " + filasProcesadas + ", Totales: " + totalrows  + ", Ejecucion # " + executions + ", Duracion = " + totalTime + " ms. (" + totalTime/1000 + " s).");
		}


		if(delayQuery  > 0 ){
			try {
				Thread.sleep(delayQuery*1000);
			} catch (InterruptedException e) {
				Log.logError("Process", "Error en Procesamiento (_sleep_) - Evento No procesado : " + e.getMessage());
			} 
		}

		return status;
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



	@Override
	protected Status doProcess() throws EventDeliveryException {
		return this.process();
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

	private void printSupportedCiphers() {
		SSLServerSocketFactory ssf = (SSLServerSocketFactory)SSLServerSocketFactory.getDefault();

		String[] defaultCiphers = ssf.getDefaultCipherSuites();
		String[] availableCiphers = ssf.getSupportedCipherSuites();

		TreeMap<String,Boolean> ciphers = new TreeMap<String,Boolean> ();

		for(int i=0; i<availableCiphers.length; ++i )
			ciphers.put(availableCiphers[i], Boolean.FALSE);

		for(int i=0; i<defaultCiphers.length; ++i )
			ciphers.put(defaultCiphers[i], Boolean.TRUE);



		System.out.println("Default\tCipher");
		for(Iterator<Entry<String,Boolean>> i = ciphers.entrySet().iterator(); i.hasNext(); ) {
			Map.Entry<String,Boolean> cipher=(Map.Entry<String,Boolean>)i.next();

			if(Boolean.TRUE.equals(cipher.getValue()))
				System.out.print('*');
			else
				System.out.print(' ');

			System.out.print('\t');
			System.out.println(cipher.getKey());
		}
	}

}
