package co.bancolombia.flume.hbaseserializer;

import java.util.LinkedList;
import java.util.List;



import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

/**
 * Clase que permite definir la estructura de la información que entra a HBASE desde flume <br>
 * Este serializer toma el payload o datos que llegan de un evento de flume y genera el rowkey especifico para el tipo de transacción.
 * Este es un ejemplo que utiliza información transaccional del ITC pero dependiendo de los datos el serializer tendrá que cambiar.<br><br>
 * 
 * Este codigo se toma de ejemplo de la clase SimpleHbaseEventSerializer que es el serializer por defecto que tiene flume para el sink
 * o sifon de HBase, cuya implementación se encuentra en :  <br><br>
 * 
 *  https://github.com/apache/flume/blob/FLUME-1787/flume-ng-sinks/flume-ng-hbase-sink/src/main/java/org/apache/flume/sink/hbase/SimpleHbaseEventSerializer.java
 * 
 * @author rlarios
 *
 */
public class SabanaSerializer implements HbaseEventSerializer  {
	private static String[] names;

	static{
		names = StaticValues.getColumnNames();
		System.out.println("\n\n\n ATTENTION!!!!  ====>>>   " + names.length + "\n\n\n");
	}

	byte[] columnFamily;
	byte[] payloadColumn;
	byte[] payload;
	byte[] incrementRow;
	byte[] incrementColumn;

	String[] columnNames;

	String rowPrefix = "";

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	/**
	 * Método que permite generar las acciones (o puts)  a hacer a HBASE con cada evento que llega de flume.  
	 * Un evento podría generar varias acciones por lo cual lo que se hace es devolver una lista de puts. <br><br>
	 * 
	 * En este caso particular el evento trae una transacción del ITC en el payload.  El método toma la transacción del payload 
	 *  y genera el rowkey para la fila de HBase y genera una transacción.  En este caso particular un evento de flume es equivalente
	 *   a un put de HBase pero podrían ser más.
	 * 
	 * @see org.apache.flume.sink.hbase.HbaseEventSerializer#getActions()
	 */
	
	@Override
	public List<Row> getActions() {
		List<Row> actions = new LinkedList<Row>();
		if(payloadColumn != null){
			byte[] rowKey;
			try {

				rowKey = getrowKeyFromPayload(payload);

				Put put = new Put(rowKey);

				addColumnValuestoPut(put,columnFamily,payload);

				actions.add(put);
			} catch (Exception e){
				System.out.println("\nERROR EN VALUESSS: \n");
				throw new FlumeException("Could not get row key!", e);
			}

		}
		return actions;
	}

	@SuppressWarnings("deprecation")
	private void addColumnValuestoPut(Put put, byte[] _columnFamily,
			byte[] _payload) {
		String[] values = new String(payload).split(",");

		int tam = Math.min(values.length, names.length);

		for(int i = 0; i < tam; i ++){
			//System.out.println("\nTAM: " + tam + "\n");
			put.add(columnFamily, names[i].getBytes(), values[i].getBytes());
		}

	}

	/**
	 * Método que genera un rowkey con cada transacción.  El método toma la transacción del payload 
	 *  y genera el rowkey para la fila de HBase basado en el siguiente formato: <br><br>
	 *  CANAL:yyyy-mm-dd-hhhhh:UUID.  (asi sea el mismo registro enviado dos veces se garantiza 
	 *  que si inserta)
	 *  
	 * @param payLoad los datos de la transacción separados por comas.  Array de bytes
	 * @return array de bytes con el rowkey generado a partir de la transacción.
	 */
	private byte[] getrowKeyFromPayload(byte[] payLoad) {

		String rowkey = new StringBuffer(new String(payLoad).substring(0, 50).split(",")[0]).reverse().toString();
		return rowkey.getBytes();
	}

	/**
	 * Metodo para recibir incrementos de HBASE.  En este caso particular no se usa.
	 * @see org.apache.flume.sink.hbase.HbaseEventSerializer#getIncrements()
	 */
	@Override
	public List<Increment> getIncrements() {
		List<Increment> incs = new LinkedList<Increment>();
		if(incrementColumn != null) {
			Increment inc = new Increment(incrementRow);
			inc.addColumn(columnFamily, incrementColumn, 1);
			incs.add(inc);
		}
		return incs;
	}

	/**
	 * Metodo que inicializa el serializer.  Recibe el evento de flume y carga las variables de payload y la familia de columnas.
	 * @param event Evento recibido de flume
	 * @param cf Array de bytes del nombre de la familia de columnas de HBase donde se ingresarán los datos.
	 * @see org.apache.flume.sink.hbase.HbaseEventSerializer#initialize(org.apache.flume.Event, byte[])
	 */
	@Override
	public void initialize(Event event, byte[] cf) {
		this.payload = event.getBody();
		this.columnFamily = cf;

	}

	/**
	 * Metodo que configura el serializer.  Recibe el contexto de flume (con los parametros que uno quiera definir.<br>
	 * Este contexto recibe los datos de rowPrefix, incrementRow, payloadColumn, incrementColumn.
	 * @param context contexto recibido por parte de Flume
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		rowPrefix = context.getString("rowPrefix", "default");
		incrementRow =
				context.getString("incrementRow", "incRow").getBytes();

		String payloadColumn = context.getString("payloadColumn","pCol");
		String incColumn = context.getString("incrementColumn","iCol");

		if(payloadColumn != null && !payloadColumn.isEmpty()) {

			this.payloadColumn = payloadColumn.getBytes();
		}

		if(incColumn != null && !incColumn.isEmpty()) {
			incrementColumn = incColumn.getBytes();
		}
	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		columnNames =  StaticValues.getColumnNames();

	}



}
