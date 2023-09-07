package co.bancolombia.flume.sources.sql;


import java.sql.Connection;

import java.sql.SQLException;
import java.util.Properties;

import co.bancolombia.flume.util.Log;


import org.apache.commons.dbcp.BasicDataSource;



/**
 * Clase que sirve como front para usar el pool de conexiones de Apache.  Facilita la creación de los pool de conexiones.
 * 
 * @author rlarios
 *
 */
public class ConnectionPool {

	private BasicDataSource  cpds;




	private int numbConn;
	private int maxConn;
	Properties prop = null;

	/**
	 * Constructor del Pool de conexiones. Se encarga de crear la instancia del pool de conexiones para ser usado por los Agentes que lo requieran
	 * @param conn_numb El número de conexiiones que se van a crear en el pool
	 * @param maxConn el número máximo de conexiones del pool
	 * @param driver driver de conexión a la base de datos
	 * @param url url de conexión a la base de datos
	 * @param usr usuario de conexión a la base de datos
	 * @param pwd contraseña de conexión a la base de datos
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public ConnectionPool(int conn_numb, int maxConn , String driver, String url, String usr, String pwd) throws ClassNotFoundException, SQLException{
		numbConn = conn_numb;
		this.maxConn = maxConn;

		setProperties(driver, url, usr, pwd);

		cpds = new BasicDataSource();

		initialize();


	}

	/**
	 * Inicializa el pool de conexiones.  Genera todas las conexiones necesarias de acuerdo a la configuraciones enviadas
	 * 
	 * @throws ClassNotFoundException En el caso de que el driver no haya sido encontrado en el classpath
	 * @throws SQLException En el caso que haya problemas generando las conexiones.
	 */
	private void initialize() throws ClassNotFoundException, SQLException {


		synchronized (this) {
			/*   esto es en C3P=  Otro pool anterior que se quiso utilizar
			try {

				cpds.setDriverClass( prop.getProperty("driver") );
			} catch (PropertyVetoException e) {
				throw new ClassNotFoundException("Error en el Driver Enviado : " + prop.getProperty("driver") );
			} //loads the jdbc driver            
			cpds.setJdbcUrl( prop.getProperty("url")  );
			cpds.setUser(prop.getProperty("usr") );                                  
			cpds.setPassword(prop.getProperty("pass")); 
			cpds.setMinPoolSize(numbConn);
			cpds.setAcquireIncrement(2);
	        cpds.setMaxPoolSize(maxConn); */

			/* ESTO ES EN DBCP  */
			cpds.setUrl( prop.getProperty("url")  );
			cpds.setDriverClassName( prop.getProperty("driver") );
			cpds.setUsername(prop.getProperty("usr"));
			cpds.setPassword(prop.getProperty("pass"));

			cpds.setMinIdle(numbConn);
			cpds.setMaxIdle(maxConn);
			cpds.setMaxOpenPreparedStatements(100);


		}

		//System.out.println("Connection Pool is Set.");
		Log.logInfo("ConnectionPool-initialize", "Connection Pool is Set.");

	}



	/**
	 * Trae una conexión del pool de conexiones
	 * 
	 * @return La Conexión disponible del poool
	 * @throws SQLException en el caso que haya errores en la generación de una nueva conexión
	 */
	public Connection getConnection() throws SQLException{

		return cpds.getConnection();

	}

	/**
	 * Devuelve una conexión al pool
	 * @param connection la conexión a devolver
	 */
	public synchronized void returnConnectionToPool(Connection connection)
	{
		//cpds.setC do nothing???
		try {
			connection.close();
		} catch (SQLException e) {
			//System.err.println("[Stop] Error Cerrando la conexión Especifica (SQLException) : " + e.getMessage());
			Log.logError("ConnectionPool-returnConnectionToPool", "Error Cerrando la conexión Especifica (SQLException) : " + e.getMessage());
		}
	}

	/**
	 * Cierra el pool junto a todas sus conexiones.
	 * @throws SQLException En el caso de que haya problemas cerrando las conexiones.
	 */
	public synchronized void closeAllConnections() throws SQLException{

		cpds.close();
	}

	/**
	 * Establece las propiedades para crear el pool
	 * 
	 * @param driver driver de conexión a la base de datos
	 * @param url url de conexión a la base de datos
	 * @param usr usuario de conexión a la base de datos
	 * @param pass contraseña de conexión a la base de datos
	 */
	private void setProperties(String driver, String url, String usr, String pass) {
		prop = new Properties();
		prop.setProperty("driver", driver);
		prop.setProperty("url", url);
		prop.setProperty("usr", usr);
		prop.setProperty("pass", pass);

	}

}
