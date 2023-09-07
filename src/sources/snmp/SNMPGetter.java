/***************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one 
 * or more contributor license agreements.  See the NOTICE file 
 * distributed with this work for additional information 
 * regarding copyright ownership.  The ASF licenses this file 
 * to you under the Apache License, Version 2.0 (the 
 * "License"); you may not use this file except in compliance 
 * with the License.  You may obtain a copy of the License at 
 *   
 * http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY 
 * KIND, either express or implied.  See the License for the 
 * specific language governing permissions and limitations 
 * under the License. 
 ****************************************************************/
package co.bancolombia.flume.sources.snmp;

import java.io.IOException; 
import java.util.Vector; 
import java.util.Map; 
import java.util.HashMap; 
import java.util.Date; 
import java.text.DateFormat; 
import java.text.SimpleDateFormat; 

import org.apache.flume.ChannelException; 
import org.apache.flume.CounterGroup; 
import org.apache.flume.Context; 
import org.apache.flume.Event; 
import org.apache.flume.event.SimpleEvent; 
import org.apache.flume.EventDeliveryException; 
import org.apache.flume.PollableSource; 
import org.apache.flume.conf.Configurable; 
import org.apache.flume.source.AbstractSource; 

import org.snmp4j.CommunityTarget; 
import org.snmp4j.PDU; 
import org.snmp4j.Snmp; 

import org.snmp4j.event.ResponseEvent; 
import org.snmp4j.mp.SnmpConstants; 

import org.snmp4j.smi.OID; 
import org.snmp4j.smi.OctetString; 
import org.snmp4j.smi.UdpAddress; 
import org.snmp4j.smi.VariableBinding; 
import org.snmp4j.transport.DefaultUdpTransportMapping; 


/**
 * Esto es un Flume Custom Source, el cual su funcion es hacer GETS a través de protocolo SNMP
 * a un dispositivo o máquina.  Este custom Source utiliza las librerias de SNMP4j para realizar la comunicación a través de SNMP.
 * 
 * <br><br>Para configurar el Agente en flume se deben utilizar los siguientes parámetros en el archivo
 * de configuración de Flume:
 * 
 * <li> bindAddress (Obligatorio) : nombre o dirección IP de host al cual se le hará la consulta SNMP  
 * <li> bindPort (Default=161) : Puerto al cual esta escuchando el protocolo SNMP 
 * <li> delayQuery (Default=30) : Valor en segundos de cada cuando búsca la información  
 * <li> community (Default=public) : Nombre de la comunidad en que se registra el servicio SNMP
 * <li> identifier (Default=SNMPAgent-<em>timestamp</em>) : Nombre con el cual quiere identificar al agente  
 * <li> verbose (Default=false) : ponga true si quiere que cada evento que envie lo muestre en la pantalla de salida.
 * <li> Lista de OIDs : Se entrega la lista de OIDs para buscar información en el host.  Se debe configurar como oid1, oid2, oid3, etc.  Ejemplo:
 * <ul> a1.sources.r1.oid1 = .1.3.6.1.2.1.25.2.3.1.6 <br> a1.sources.r1.oid2 = .1.3.6.1.2.1.25.2.2.0</ul>
 *
 * El código se tomó en su mayoría de la siguiente implementación. https://github.com/javiroman/flume-snmp-source
 * <br>Se le realizaron cambios para añadir configurciones adicionales, como nombre de comunidad, delay, identificador
 * y posibilidad de añadir verbosidad. 
 * 
 * @author rlarios
 *
 */
public class SNMPGetter extends AbstractSource implements  
Configurable, PollableSource { 

	private String bindAddress;
	private String identifier;
	private int bindPort; 
	private int version = 1;
	private int delayQuery; 

	private PDU pdu; 
	private CommunityTarget target; 
	private Snmp snmp; 
	private CounterGroup counterGroup; 
	private static final int DEFAULT_PORT = 161; 
	private static final int DEFAULT_DELAY = 30; // seconds 
	private String community;

	private boolean verbose = false;



	@Override 
	public void start() { 
		// Initialize the connection to the external client 
		try { 
			snmp = new Snmp(new DefaultUdpTransportMapping()); 
			snmp.listen(); 

			target = new CommunityTarget(); 
			target.setCommunity(new OctetString(community)); 
			target.setVersion(getVersion(version)); 
			target.setAddress(new UdpAddress(bindAddress + "/" + bindPort)); 
			target.setTimeout(3000);    //3s 
			target.setRetries(1); 

			pdu.setType(PDU.GETBULK); 
			pdu.setMaxRepetitions(1);  
			pdu.setNonRepeaters(0); 

		} catch (IOException ex) { 
			System.out.println("ERROR en Inicio agente (" + identifier + "): " + ex.getMessage()); 
		} 

		super.start(); 
	} 

	private int getVersion(int version2) {
		int ret = 0;
		if(version == 1){ 
			ret = SnmpConstants.version1;
		}else{
			if(version == 2){
				ret = SnmpConstants.version2c;
			}else{
				if(version == 3){
					ret = SnmpConstants.version3;
				}{
					System.out.println("Se envio una version Distinta a la esperada = " + version + ", se espera valores 1, 2 o 3");
				}
			}
		}

		return ret;
	}

	@Override 
	public void stop() { 
		System.out.println("Deteniendo FlumeSource SNMPQuery"); 
		System.out.println("Metricas:{}" + counterGroup); 

		super.stop(); 
	} 

	@Override 
	public Status process() throws EventDeliveryException { 
		Status status = null; 
		counterGroup = new CounterGroup(); 
		boolean goodResponse = false;
		try { 

			Event event = new SimpleEvent();  
			Map <String, String> headers = new HashMap<String, String>(); 
			StringBuilder stringBuilder = new StringBuilder(); 

			ResponseEvent responseEvent = snmp.send(pdu, target); 
			PDU response = responseEvent.getResponse(); 

			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
			Date date = new Date(); 

			stringBuilder.append(identifier + ","); 
			stringBuilder.append(dateFormat.format(date) + ","); 
			stringBuilder.append(bindAddress + ","); 

			if (response == null) { 
				System.out.println("La respuesta es Nula (Null)"); 
			} else { 
				if (response.getErrorStatus() == PDU.noError) {
					goodResponse = true;
					Vector<? extends VariableBinding> vbs = response.getVariableBindings(); 
					for (VariableBinding vb : vbs) { 

						stringBuilder.append(vb.getVariable().toString() + ","); 
					} 
				} else { 
					System.out.println("Respuesta con Error:" + response.getErrorStatusText()); 
				} 
			} 

			if(goodResponse){
				String messageString = stringBuilder.toString(); 

				messageString = messageString.substring(0, messageString.lastIndexOf(","));

				if(verbose)
					System.out.println("Evento : " +  messageString );


				byte[] message = messageString.getBytes(); 

				headers.put("timestamp", String.valueOf(System.currentTimeMillis())); 

				event.setBody(message); 
				event.setHeaders(headers); 

				getChannelProcessor().processEvent(event); 
				counterGroup.incrementAndGet("events.success");
			}else{
				System.out.println("No hubo buena Respuesta :( ... durmiendo"); 
			}


			if(delayQuery > 0 ){
				Thread.sleep(delayQuery*1000); 
			}

			status = Status.READY; 

		} catch (ChannelException|IOException|InterruptedException ex) { 
			counterGroup.incrementAndGet("events.dropped"); 

			System.out.println("Error en Procesamiento (" + identifier + ") - Evento No procesado : " + ex.getMessage() );
			status = Status.BACKOFF; 

		} 
		return status; 
	} 

	@Override 
	public void configure(Context context) { 

		String baseString = "oid"; 
		boolean notFound = true; 
		int i = 0; 

		//parameters = context.getParameters(); 

		bindAddress = context.getString("host"); 
		bindPort = context.getInteger("port", DEFAULT_PORT); 
		delayQuery = context.getInteger("delay", DEFAULT_DELAY); 
		community = context.getString("community", "public");
		identifier = context.getString("identifier", "SNMPAgent-" + System.currentTimeMillis());
		version = context.getInteger("version",1);
		verbose = context.getBoolean("verbose", Boolean.FALSE);

		System.out.println("parametro: (bindAddress) : " + bindAddress); 
		System.out.println("parametro: (bindPort) : " + bindPort); 
		System.out.println("parametro: (community) : " + community); 
		System.out.println("parametro: (delayQuery) : " + delayQuery);
		System.out.println("parametro: (identifier) : " + identifier); 
		System.out.println("parametro: (verbose) : " + verbose);
		System.out.println("parametro: (version) : " + version);
		System.out.println("Lista de OIDs para agente: " + identifier);

		pdu = new PDU(); 

		do { 
			i++ ; 
			if (context.getString(baseString + i) == null) { 
				notFound = false; 
			} else { 
				System.out.println("parametro: (" + (baseString + i) + ") : " + context.getString(baseString + i));  
				pdu.add(new VariableBinding(new OID(context.getString(baseString + i))));  
			} 
		} while (notFound); 

		System.out.println("");

	} 
}
